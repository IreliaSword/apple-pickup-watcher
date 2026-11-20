//! All network fixtures are loopback proxies; no Apple or public-proxy traffic.
use super::*;
use crate::model::{Category, Family};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const PART: &str = "MG724CH/A";
const REGION: Region = Region {
    locale: "zh_CN",
    title: "offline",
    base_url: "http://inventory.invalid",
    families: &[Family {
        category: Category::Iphone,
        slug: "iphone-18-pro",
    }],
};

fn body(display: &str) -> String {
    serde_json::json!({"head":{"status":"200"},"body":{"stores":[{
        "storeNumber":"R000","storeName":"offline",
        "partsAvailability":{PART:{"partNumber":PART,"pickupDisplay":display}}
    }]}})
    .to_string()
}

#[derive(Clone)]
enum Reply {
    Http(u16, String),
    Close,
    Timeout,
    Gated {
        entered: Arc<Notify>,
        release: Arc<Notify>,
    },
}

struct Proxy {
    url: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn proxy(id: usize, reply: Reply, seen: Arc<std::sync::Mutex<Vec<usize>>>) -> Proxy {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let reply = reply.clone();
            let seen = seen.clone();
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut chunk = [0; 2048];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    let n = socket.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 || request.len() > 16_384 {
                        return;
                    }
                    request.extend_from_slice(&chunk[..n]);
                }
                seen.lock().unwrap().push(id);
                match reply {
                    Reply::Http(status, body) => {
                        let response = format!(
                            "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = socket.write_all(response.as_bytes()).await;
                    }
                    Reply::Close => {}
                    Reply::Timeout => tokio::time::sleep(Duration::from_millis(300)).await,
                    Reply::Gated { entered, release } => {
                        entered.notify_one();
                        release.notified().await;
                        let body = body("available");
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = socket.write_all(response.as_bytes()).await;
                    }
                }
            });
        }
    });
    Proxy { url, task }
}

fn client() -> AppleClient {
    AppleClient::new(ClientConfig {
        automatic_proxy_pool: true,
        transport: Transport::Rustls,
        warm_page: WarmPage::None,
        budget: Budget::unlimited(),
        min_interval: Duration::ZERO,
        max_retries: 5,
        timeout: Duration::from_millis(100),
        ..ClientConfig::default()
    })
    .unwrap()
}

async fn install(client: &AppleClient, proxies: &[Proxy]) {
    let pool = ProxyPool::fixture(&proxies.iter().map(|p| p.url.clone()).collect::<Vec<_>>());
    let mut auto = client.automatic.lock().await;
    client.install_automatic_routes(&pool).await.unwrap();
    auto.as_mut().unwrap().pool = pool;
}

async fn check(client: &AppleClient) -> Result<StoreAvailability, ApiError> {
    tokio::time::timeout(
        Duration::from_secs(2),
        client.pickup_message(&REGION, "R000", &[PART.into()]),
    )
    .await
    .expect("fallback must not wait for polling/same-route retries")
}

#[tokio::test]
async fn success_rotates_and_failed_b_is_replaced_in_b_not_at_the_tail() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut proxies = Vec::new();
    for id in 0..5 {
        let reply = if id == 1 {
            Reply::Http(541, "{}".into())
        } else {
            Reply::Http(200, body("available"))
        };
        proxies.push(proxy(id, reply, seen.clone()).await);
    }
    let client = client();
    install(&client, &proxies).await;
    check(&client).await.unwrap(); // A
    check(&client).await.unwrap(); // B fails, immediately C
    assert_eq!(*seen.lock().unwrap(), [0, 1, 2]);
    let entries = client.automatic_pool_entries().await.unwrap();
    assert_eq!(
        entries.iter().map(|e| e.slot).collect::<Vec<_>>(),
        [0, 2, 3, 4]
    );
    assert!(entries[1].responded);
    let c_route = client.routes.read().await[1].clone();
    proxies[1] = proxy(5, Reply::Http(200, body("unavailable")), seen.clone()).await;
    install(&client, &proxies).await; // background replacement B'
    assert!(Arc::ptr_eq(&c_route, &client.routes.read().await[2]));
    for _ in 0..4 {
        check(&client).await.unwrap();
    } // D E A B'
    assert_eq!(*seen.lock().unwrap(), [0, 1, 2, 3, 4, 0, 5]);
}

#[tokio::test]
async fn every_actual_failure_evicts_once_without_same_route_retries() {
    for failure in [
        Reply::Http(541, "{}".into()),
        Reply::Http(403, "{}".into()),
        Reply::Http(429, "{}".into()),
        Reply::Http(500, "{}".into()),
        Reply::Close,
        Reply::Timeout,
        Reply::Http(200, "not json".into()),
        Reply::Http(200, "{}".into()),
        Reply::Http(200, body("unexpected-status")),
    ] {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let proxies = vec![
            proxy(0, failure, seen.clone()).await,
            proxy(1, Reply::Http(200, body("unavailable")), seen.clone()).await,
        ];
        let client = client();
        install(&client, &proxies).await;
        let result = check(&client).await.unwrap();
        assert_eq!(
            result.parts[PART].availability,
            crate::model::Availability::OutOfStock
        );
        assert_eq!(*seen.lock().unwrap(), [0, 1]);
        assert_eq!(
            client
                .automatic_pool_entries()
                .await
                .unwrap()
                .iter()
                .map(|e| e.slot)
                .collect::<Vec<_>>(),
            [1]
        );
    }
}

#[tokio::test]
async fn c_failure_immediately_tries_d_and_empty_slots_are_skipped() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut proxies = Vec::new();
    for id in 0..5 {
        proxies.push(
            proxy(
                id,
                if [1, 2].contains(&id) {
                    Reply::Close
                } else {
                    Reply::Http(200, body("available"))
                },
                seen.clone(),
            )
            .await,
        );
    }
    let client = client();
    install(&client, &proxies).await;
    for _ in 0..5 {
        check(&client).await.unwrap();
    }
    assert_eq!(*seen.lock().unwrap(), [0, 1, 2, 3, 4, 0, 3]);
}

#[tokio::test]
async fn all_failed_and_empty_pools_return_bounded_errors_without_direct_fallback() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut proxies = Vec::new();
    for id in 0..5 {
        proxies.push(proxy(id, Reply::Close, seen.clone()).await);
    }
    let client = client();
    assert!(check(&client).await.is_err());
    assert!(client.routes.read().await.is_empty());
    install(&client, &proxies).await;
    assert!(check(&client).await.is_err());
    assert!(check(&client).await.is_err());
    assert_eq!(*seen.lock().unwrap(), [0, 1, 2, 3, 4]);
    assert!(client.routes.read().await.is_empty());
    assert!(client.automatic_pool_entries().await.unwrap().is_empty());
}

#[tokio::test]
async fn concurrent_logical_queries_preserve_next_slot_order() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut proxies = Vec::new();
    for id in 0..5 {
        proxies.push(proxy(id, Reply::Http(200, body("available")), seen.clone()).await);
    }
    let client = client();
    install(&client, &proxies).await;
    let (a, b, c, d, e) = tokio::join!(
        check(&client),
        check(&client),
        check(&client),
        check(&client),
        check(&client)
    );
    for result in [a, b, c, d, e] {
        result.unwrap();
    }
    assert_eq!(*seen.lock().unwrap(), [0, 1, 2, 3, 4]);
}

#[tokio::test]
async fn refill_reuses_sessions_and_stale_completion_cannot_modify_new_generation() {
    let client = client();
    let candidates: Vec<_> = (1..=9)
        .map(|i| Candidate {
            url: format!("http://8.8.8.{i}:8080"),
            source: "offline-no-connect".into(),
            uptime: None,
            latency_ms: Some(f64::from(i)),
        })
        .collect();
    client
        .replenish_automatic_pool(candidates.clone())
        .await
        .unwrap();
    let routes = client.routes.read().await.clone();
    let generation = client.automatic.lock().await.as_ref().unwrap().generation;
    client
        .finish_automatic_attempt(generation, &routes[1], false)
        .await;
    let changed =
        tokio::time::timeout(Duration::from_millis(20), client.automatic_pool_changed()).await;
    assert!(changed.is_ok());
    let entries = client.replenish_automatic_pool(candidates).await.unwrap();
    assert_eq!(entries[1].slot, 1);
    assert_eq!(entries[1].url, "http://8.8.8.6:8080");
    let new_routes = client.routes.read().await.clone();
    for i in [0, 2, 3, 4] {
        assert!(Arc::ptr_eq(&routes[i], &new_routes[i]));
    }
    assert!(!Arc::ptr_eq(&routes[1], &new_routes[1]));
    assert_ne!(routes[1].label, new_routes[1].label);
    client.enable_automatic_pool().await;
    client
        .finish_automatic_attempt(generation, &routes[0], true)
        .await;
    assert!(client.automatic_pool_entries().await.unwrap().is_empty());
    client.set_proxies(&[]).await.unwrap();
    assert!(client.automatic_pool_entries().await.is_none());
    assert!(
        client
            .replenish_automatic_pool(vec![])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(client.routes.read().await.len(), 1); // explicit manual mode only
}

#[tokio::test]
async fn locally_cooled_routes_are_skipped_not_evicted_or_counted_as_actual_failures() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let proxies = vec![
        proxy(0, Reply::Http(200, body("available")), seen.clone()).await,
        proxy(1, Reply::Http(200, body("available")), seen.clone()).await,
    ];
    let client = client();
    install(&client, &proxies).await;
    client.routes.read().await[0]
        .blocks
        .lock()
        .await
        .record(REGION.locale, Instant::now(), false);
    check(&client).await.unwrap();
    assert_eq!(*seen.lock().unwrap(), [1]);
    assert_eq!(client.automatic_pool_entries().await.unwrap().len(), 2);
}

#[tokio::test]
async fn refilled_slots_cannot_be_retried_in_the_same_logical_query() {
    let client = client();
    client
        .replenish_automatic_pool(
            (1..=6)
                .map(|i| Candidate {
                    url: format!("http://8.8.8.{i}:8080"),
                    source: "offline-no-connect".into(),
                    uptime: None,
                    latency_ms: None,
                })
                .collect(),
        )
        .await
        .unwrap();
    let generation = client.automatic.lock().await.as_ref().unwrap().generation;
    let (a, slot, _) = client
        .pick_automatic_route(&REGION, generation, &[])
        .await
        .unwrap();
    assert_eq!(slot, 0);
    client.finish_automatic_attempt(generation, &a, false).await;
    client
        .replenish_automatic_pool(vec![Candidate {
            url: "http://9.9.9.9:8080".into(),
            source: "replacement".into(),
            uptime: None,
            latency_ms: None,
        }])
        .await
        .unwrap();
    assert!(
        client
            .pick_automatic_route(&REGION, generation, &[0, 1, 2, 3, 4])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn next_route_runs_before_background_refill_and_refill_can_finish_during_query() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut proxies = vec![
        proxy(0, Reply::Close, seen.clone()).await,
        proxy(
            1,
            Reply::Gated {
                entered: entered.clone(),
                release: release.clone(),
            },
            seen.clone(),
        )
        .await,
    ];
    let client = AppleClient::new(ClientConfig {
        timeout: Duration::from_secs(2),
        ..client().config
    })
    .unwrap();
    install(&client, &proxies).await;
    let query_client = client.clone();
    let query = tokio::spawn(async move { check(&query_client).await });
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .unwrap();
    assert_eq!(
        *seen.lock().unwrap(),
        [0, 1],
        "fallback already sent while maintainer has not run"
    );
    assert_eq!(client.automatic_pool_entries().await.unwrap().len(), 1);
    proxies[0] = proxy(5, Reply::Http(200, body("available")), seen.clone()).await;
    install(&client, &proxies).await;
    release.notify_one();
    query.await.unwrap().unwrap();
    assert_eq!(client.automatic_pool_entries().await.unwrap()[0].slot, 0);
    check(&client).await.unwrap();
    assert_eq!(*seen.lock().unwrap(), [0, 1, 5]);
}

#[tokio::test]
async fn failed_fallback_still_obeys_global_spacing_and_success_keeps_route_budget() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let proxies = vec![
        proxy(0, Reply::Close, seen.clone()).await,
        proxy(1, Reply::Http(200, body("available")), seen.clone()).await,
    ];
    let client = AppleClient::new(ClientConfig {
        min_interval: Duration::from_millis(60),
        ..client().config
    })
    .unwrap();
    install(&client, &proxies).await;
    let start = Instant::now();
    check(&client).await.unwrap();
    assert!(start.elapsed() >= Duration::from_millis(55));
    let client = AppleClient::new(ClientConfig {
        min_interval: Duration::ZERO,
        budget: Budget {
            capacity: 1,
            refill_every: Duration::from_millis(60),
        },
        ..client.config.clone()
    })
    .unwrap();
    install(&client, &proxies[1..]).await;
    let start = Instant::now();
    check(&client).await.unwrap();
    check(&client).await.unwrap();
    assert!(start.elapsed() >= Duration::from_millis(55));
}

#[tokio::test]
async fn pickup_not_open_is_success_and_missing_requested_part_is_not_success() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let missing = body("available").replace(PART, "OTHER-PART");
    let proxies = vec![
        proxy(0, Reply::Http(200, missing), seen.clone()).await,
        proxy(1, Reply::Http(200, body("default")), seen.clone()).await,
    ];
    let client = client();
    install(&client, &proxies).await;
    let result = check(&client).await.unwrap();
    assert!(matches!(
        result.parts[PART].availability,
        Availability::Unknown(UnknownReason::PickupNotOpen { .. })
    ));
    assert_eq!(*seen.lock().unwrap(), [0, 1]);
    assert_eq!(client.automatic_pool_entries().await.unwrap().len(), 1);
}

#[tokio::test]
async fn nearby_queries_use_the_same_typed_fast_fallback() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let proxies = vec![
        proxy(0, Reply::Http(200, body("unexpected-status")), seen.clone()).await,
        proxy(1, Reply::Http(200, body("unavailable")), seen.clone()).await,
    ];
    let client = client();
    install(&client, &proxies).await;
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        client.pickup_message_nearby(&REGION, "offline-location", &[PART.into()]),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result[0].parts[PART].availability, Availability::OutOfStock);
    assert_eq!(*seen.lock().unwrap(), [0, 1]);
}
