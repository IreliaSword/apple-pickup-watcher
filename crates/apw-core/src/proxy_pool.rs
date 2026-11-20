//! Bounded candidate pool. No network IO or inventory decisions here.
use crate::apple::RequestRecord;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;

pub const TARGET_SIZE: usize = 5;
const REJECT_FOR_MS: u64 = 30 * 60 * 1000;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Candidate {
    pub url: String,
    pub source: String,
    #[serde(default)]
    pub uptime: Option<f64>,
    #[serde(default)]
    pub latency_ms: Option<f64>,
}

/// Untrusted feeds cannot inject credentials or connect to local services.
/// Manual proxies are deliberately not subject to this restriction.
pub fn valid_public_proxy(raw: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(raw) else {
        return false;
    };
    if !matches!(url.scheme(), "http" | "https" | "socks5" | "socks5h")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
        || url.port_or_known_default().unwrap_or(0) == 0
    {
        return false;
    }
    let Some(host) = url.host_str() else {
        return false;
    };
    let Ok(ip) = host.trim_matches(['[', ']']).parse::<IpAddr>() else {
        return false;
    };
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !(a == 0
                || a == 10
                || a == 127
                || a >= 224
                || (a == 100 && (64..=127).contains(&b))
                || (a == 169 && b == 254)
                || (a == 172 && (16..=31).contains(&b))
                || (a == 192 && (b == 168 || (b == 0 && (c == 0 || c == 2))))
                || (a == 198 && (b == 18 || b == 19 || (b == 51 && c == 100)))
                || (a == 203 && b == 0 && c == 113))
        }
        IpAddr::V6(ip) => {
            let s = ip.segments();
            (s[0] & 0xe000) == 0x2000 && !(s[0] == 0x2001 && s[1] == 0x0db8)
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolEntry {
    pub url: String,
    pub source: String,
    /// Consecutive actual rounds ending in transport failure or HTTP 541.
    pub failure_rounds: u8,
    /// A response is not evidence of available stock.
    pub responded: bool,
}

#[derive(Clone, Default, Debug)]
pub struct ProxyPool {
    entries: Vec<PoolEntry>,
    rejected: HashMap<String, u64>,
    cursor: u64,
}

impl ProxyPool {
    pub fn entries(&self) -> &[PoolEntry] {
        &self.entries
    }
    pub fn urls(&self) -> Vec<String> {
        self.entries.iter().map(|e| e.url.clone()).collect()
    }

    /// Fill vacancies only; never reset healthy entries during a feed refresh.
    pub fn replenish(&mut self, mut candidates: Vec<Candidate>, now_ms: u64) {
        self.rejected.retain(|_, until| *until > now_ms);
        candidates.retain(|c| {
            valid_public_proxy(&c.url)
                && c.uptime
                    .is_none_or(|x| x.is_finite() && (0.0..=100.0).contains(&x))
                && c.latency_ms.is_none_or(|x| x.is_finite() && x >= 0.0)
        });
        candidates.sort_by(|a, b| {
            b.uptime
                .unwrap_or(-1.0)
                .total_cmp(&a.uptime.unwrap_or(-1.0))
                .then_with(|| {
                    a.latency_ms
                        .unwrap_or(f64::MAX)
                        .total_cmp(&b.latency_ms.unwrap_or(f64::MAX))
                })
                .then_with(|| a.url.cmp(&b.url))
        });
        let mut hosts: HashSet<String> = self
            .entries
            .iter()
            .filter_map(|e| {
                reqwest::Url::parse(&e.url)
                    .ok()?
                    .host_str()
                    .map(str::to_owned)
            })
            .collect();
        for candidate in candidates {
            if self.entries.len() >= TARGET_SIZE {
                break;
            }
            if self.rejected.contains_key(&candidate.url) {
                continue;
            }
            let Ok(url) = reqwest::Url::parse(&candidate.url) else {
                continue;
            };
            let Some(host) = url.host_str() else { continue };
            if !hosts.insert(host.to_owned()) {
                continue;
            }
            self.entries.push(PoolEntry {
                url: candidate.url,
                source: candidate.source,
                failure_rounds: 0,
                responded: false,
            });
        }
    }

    /// Called once per completed watcher round. The last actual pickup result
    /// counts once: transport failure or HTTP 541 increments the streak; other
    /// HTTP responses reset it. Local cooldowns cannot advance the streak.
    pub fn observe_round(
        &mut self,
        records: &[RequestRecord],
        routes: &[(String, String)],
        now_ms: u64,
    ) -> bool {
        let mut last = HashMap::<&str, &RequestRecord>::new();
        for record in records.iter().filter(|r| r.sequence > self.cursor) {
            if record.kind != "pickup" {
                continue;
            }
            match last.get(record.route.as_str()) {
                Some(old) if old.sequence >= record.sequence => {}
                _ => {
                    last.insert(&record.route, record);
                }
            }
        }
        self.cursor = records
            .iter()
            .map(|r| r.sequence)
            .max()
            .unwrap_or(self.cursor)
            .max(self.cursor);
        for (label, url) in routes {
            let Some(record) = last.get(label.as_str()) else {
                continue;
            };
            let Some(entry) = self.entries.iter_mut().find(|e| &e.url == url) else {
                continue;
            };
            match record.status {
                Some(541) => {
                    entry.failure_rounds = entry.failure_rounds.saturating_add(1);
                    entry.responded = true;
                }
                Some(_) => {
                    entry.failure_rounds = 0;
                    entry.responded = true;
                }
                None if record.outcome == "transport" => {
                    entry.failure_rounds = entry.failure_rounds.saturating_add(1);
                    entry.responded = false;
                }
                None => {}
            }
        }
        let before = self.entries.len();
        self.entries.retain(|entry| {
            if entry.failure_rounds < 2 {
                return true;
            }
            self.rejected
                .insert(entry.url.clone(), now_ms.saturating_add(REJECT_FOR_MS));
            false
        });
        self.entries.len() != before
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn candidate(n: u8, uptime: f64) -> Candidate {
        Candidate {
            url: format!("http://8.8.8.{n}:8080"),
            source: "offline-fixture".into(),
            uptime: Some(uptime),
            latency_ms: Some(20.0),
        }
    }
    fn record(sequence: u64, route: &str, status: Option<u16>) -> RequestRecord {
        RequestRecord {
            sequence,
            at_ms: 0,
            kind: "pickup",
            transport: "rustls",
            route: route.into(),
            locale: "ja_JP".into(),
            store: None,
            location: None,
            parts: 1,
            status,
            http_version: None,
            duration_ms: 1,
            outcome: if status.is_some() {
                "blocked"
            } else {
                "transport"
            }
            .into(),
            cookies_sent: vec![],
            cookies_after: vec![],
            final_url: None,
        }
    }
    #[test]
    fn filters_untrusted_endpoints() {
        for url in [
            "http://127.0.0.1:80",
            "http://10.0.0.1:80",
            "http://169.254.169.254:80",
            "http://100.64.0.1:80",
            "http://user:pass@8.8.8.8:80",
            "http://localhost:80",
            "http://8.8.8.8:80/path",
            "socks4://8.8.8.8:1080",
            "http://[::1]:80",
            "http://[::ffff:127.0.0.1]:80",
        ] {
            assert!(!valid_public_proxy(url), "{url}");
        }
        assert!(valid_public_proxy("socks5h://8.8.8.8:1080"));
        assert!(valid_public_proxy("http://8.8.8.8:80"));
        assert!(valid_public_proxy("https://8.8.8.8:443"));
    }
    #[test]
    fn best_uptime_first_and_five_distinct_hosts() {
        let mut pool = ProxyPool::default();
        pool.replenish(
            (1..=8).map(|n| candidate(n, f64::from(n) + 90.0)).collect(),
            0,
        );
        assert_eq!(pool.entries.len(), 5);
        assert_eq!(pool.entries[0].url, candidate(8, 98.0).url);
    }
    #[test]
    fn retries_one_round_count_once_and_only_replace_bad_entry() {
        let mut pool = ProxyPool::default();
        let all: Vec<_> = (1..=6).map(|n| candidate(n, 100.0)).collect();
        pool.replenish(all.clone(), 0);
        let routes = vec![("proxy#1".into(), all[0].url.clone())];
        assert!(!pool.observe_round(
            &[record(1, "proxy#1", None), record(2, "proxy#1", None)],
            &routes,
            1
        ));
        assert_eq!(pool.entries[0].failure_rounds, 1);
        assert!(!pool.observe_round(&[record(2, "proxy#1", None)], &routes, 2));
        assert!(pool.observe_round(&[record(3, "proxy#1", None)], &routes, 3));
        pool.replenish(all.clone(), 4);
        assert_eq!(
            pool.urls(),
            all[1..].iter().map(|x| x.url.clone()).collect::<Vec<_>>()
        );
    }
    #[test]
    fn two_http_541_rounds_replace_only_the_failed_route() {
        let mut pool = ProxyPool::default();
        let all: Vec<_> = (1..=6).map(|n| candidate(n, 100.0)).collect();
        pool.replenish(all.clone(), 0);
        let routes = vec![("p".into(), all[0].url.clone())];
        let first_round = [record(1, "p", Some(541)), record(2, "p", Some(541))];
        assert!(!pool.observe_round(&first_round, &routes, 1));
        assert_eq!(pool.entries[0].failure_rounds, 1);
        assert!(pool.entries[0].responded);
        // Replayed records and a local cooldown do not create another failure.
        assert!(!pool.observe_round(&first_round, &routes, 2));
        assert!(!pool.observe_round(&[], &routes, 3));
        assert_eq!(pool.entries[0].failure_rounds, 1);
        let retained = serde_json::to_value(&pool.entries[1..]).unwrap();
        assert!(pool.observe_round(&[record(3, "p", Some(541))], &routes, 4));
        pool.replenish(all.clone(), 5);
        assert_eq!(pool.entries.len(), TARGET_SIZE);
        assert_eq!(serde_json::to_value(&pool.entries[..4]).unwrap(), retained);
        assert_eq!(pool.entries[4].url, all[5].url);
        assert!(!pool.urls().contains(&all[0].url));
    }
    #[test]
    fn transport_and_http_541_share_the_two_round_threshold() {
        for statuses in [[None, Some(541)], [Some(541), None]] {
            let mut pool = ProxyPool::default();
            let c = candidate(1, 100.0);
            pool.replenish(vec![c.clone()], 0);
            let routes = vec![("p".into(), c.url)];
            assert!(!pool.observe_round(&[record(1, "p", statuses[0])], &routes, 1));
            assert_eq!(pool.entries[0].failure_rounds, 1);
            assert!(pool.observe_round(&[record(2, "p", statuses[1])], &routes, 2));
            assert!(pool.entries.is_empty());
        }
    }
    #[test]
    fn other_http_responses_reset_the_failure_streak() {
        for status in [200, 403, 429, 500] {
            let mut pool = ProxyPool::default();
            let c = candidate(1, 100.0);
            pool.replenish(vec![c.clone()], 0);
            let routes = vec![("p".into(), c.url)];
            pool.observe_round(&[record(1, "p", Some(541))], &routes, 1);
            assert!(!pool.observe_round(&[record(2, "p", Some(status))], &routes, 2));
            assert_eq!(pool.entries[0].failure_rounds, 0);
            assert!(pool.entries[0].responded);
            assert!(!pool.observe_round(&[record(3, "p", Some(541))], &routes, 3));
            assert_eq!(pool.entries[0].failure_rounds, 1);
        }
    }
    #[test]
    fn last_actual_pickup_result_determines_each_round() {
        let mut pool = ProxyPool::default();
        let c = candidate(1, 100.0);
        pool.replenish(vec![c.clone()], 0);
        let routes = vec![("p".into(), c.url)];
        let recovered_round = [record(1, "p", Some(541)), record(2, "p", Some(200))];
        assert!(!pool.observe_round(&recovered_round, &routes, 1));
        assert_eq!(pool.entries[0].failure_rounds, 0);
        let blocked_round = [record(3, "p", Some(200)), record(4, "p", Some(541))];
        assert!(!pool.observe_round(&blocked_round, &routes, 2));
        assert_eq!(pool.entries[0].failure_rounds, 1);
        assert!(pool.observe_round(&[record(5, "p", Some(541))], &routes, 3));
    }
    #[test]
    fn http_541_eviction_excludes_the_address_for_thirty_minutes() {
        let mut pool = ProxyPool::default();
        let c = candidate(1, 100.0);
        pool.replenish(vec![c.clone()], 0);
        let routes = vec![("p".into(), c.url.clone())];
        pool.observe_round(&[record(1, "p", Some(541))], &routes, 1);
        assert!(pool.observe_round(&[record(2, "p", Some(541))], &routes, 2));
        pool.replenish(vec![c.clone()], 2 + REJECT_FOR_MS - 1);
        assert!(pool.entries.is_empty());
        pool.replenish(vec![c.clone()], 2 + REJECT_FOR_MS);
        assert_eq!(pool.urls(), vec![c.url]);
        assert_eq!(pool.entries[0].failure_rounds, 0);
    }
    #[test]
    fn status_serializes_the_combined_failure_round_count() {
        let mut pool = ProxyPool::default();
        let c = candidate(1, 100.0);
        pool.replenish(vec![c.clone()], 0);
        pool.observe_round(&[record(1, "p", Some(541))], &[("p".into(), c.url)], 1);
        let entry = serde_json::to_value(&pool.entries[0]).unwrap();
        assert_eq!(entry["failureRounds"], 1);
        assert_eq!(entry["responded"], true);
        assert!(entry.get("connectionFailures").is_none());
    }
    #[test]
    fn warm_requests_and_direct_do_not_evict() {
        let mut pool = ProxyPool::default();
        let c = candidate(1, 100.0);
        pool.replenish(vec![c.clone()], 0);
        let mut warm = record(1, "p", None);
        warm.kind = "warm";
        pool.observe_round(
            &[warm, record(2, "direct", None)],
            &[("p".into(), c.url)],
            1,
        );
        assert_eq!(pool.entries[0].failure_rounds, 0);
    }
    #[test]
    fn no_fabricated_fill_and_healthy_entries_stay() {
        let mut pool = ProxyPool::default();
        let c = candidate(1, 90.0);
        pool.replenish(vec![c.clone()], 0);
        pool.replenish(vec![candidate(2, 100.0)], 1);
        assert_eq!(pool.entries.len(), 2);
        assert_eq!(pool.entries[0].url, c.url);
    }
}
