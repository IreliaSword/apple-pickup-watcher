//! Desktop assembly for the core pool and bounded source refreshes.
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use apw_core::apple::AppleClient;
use apw_core::proxy_pool::{Candidate, PoolEntry, TARGET_SIZE};
use apw_core::proxy_sources::fetch_public_feeds;
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter};
use tokio::sync::{Mutex, Notify};

const REFRESH_MS: u64 = 5 * 60 * 1000;
const EVENT: &str = "watcher://proxy-pool";

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolStatus {
    pub enabled: bool,
    pub source: String,
    pub source_state: String,
    pub message: String,
    pub entries: Vec<PoolEntry>,
    pub target_size: usize,
    pub last_refresh_ms: Option<u64>,
}

struct State {
    candidates: Vec<Candidate>,
    status: PoolStatus,
    generation: u64,
    next_fetch: u64,
    blocked: bool,
}

#[derive(Clone)]
pub struct PoolService {
    inner: Arc<Mutex<State>>,
    wake: Arc<Notify>,
    client: AppleClient,
    profile: PathBuf,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl PoolService {
    pub fn new(client: AppleClient, enabled: bool, source: String, profile: PathBuf) -> Self {
        Self {
            inner: Arc::new(Mutex::new(State {
                candidates: vec![],
                generation: 0,
                next_fetch: 0,
                blocked: false,
                status: PoolStatus {
                    enabled,
                    source,
                    source_state: if enabled { "waiting" } else { "disabled" }.into(),
                    message: "候选不等于已验证可用；启用后库存查询不会回退直连。".into(),
                    entries: vec![],
                    target_size: TARGET_SIZE,
                    last_refresh_ms: None,
                },
            })),
            wake: Arc::new(Notify::new()),
            client,
            profile,
        }
    }

    pub async fn status(&self) -> PoolStatus {
        self.inner.lock().await.status.clone()
    }

    pub async fn configure(
        &self,
        enabled: bool,
        source: &str,
        manual: &[String],
        app: &AppHandle,
    ) -> Result<(), String> {
        let mut state = self.inner.lock().await;
        let changed = state.status.enabled != enabled || state.status.source != source;
        if changed {
            // Remove direct immediately before enabling. Manual addresses remain
            // in Settings and are restored when the feature is disabled.
            if enabled {
                self.client.enable_automatic_pool().await;
            } else {
                self.client
                    .set_proxies(manual)
                    .await
                    .map_err(|_| "代理模式切换失败".to_string())?;
            }
            state.generation += 1;
            state.candidates.clear();
            state.blocked = false;
            state.next_fetch = 0;
            state.status.enabled = enabled;
            state.status.source = source.into();
            state.status.entries.clear();
            state.status.last_refresh_ms = None;
            state.status.source_state = if enabled { "waiting" } else { "disabled" }.into();
            state.status.message = if enabled {
                "等待候选来源；当前无代理时不会直连。"
            } else {
                "自动池已关闭，恢复手动代理设置。"
            }
            .into();
        } else if !enabled {
            self.client
                .set_proxies(manual)
                .await
                .map_err(|_| "手动代理更新失败".to_string())?;
        }
        let _ = app.emit(EVENT, &state.status);
        drop(state);
        self.wake.notify_one();
        Ok(())
    }

    /// Explicit user retry only. A blocked browser source is never retried by
    /// the periodic task, and no alternate browser identity is created.
    pub async fn refresh(&self) -> Result<PoolStatus, String> {
        let mut state = self.inner.lock().await;
        if !state.status.enabled {
            return Err("请先启用自动代理池".into());
        }
        if state.status.source_state == "fetching" {
            return Err("来源正在刷新，请等待本次完成".into());
        }
        let now = now_ms();
        if state
            .status
            .last_refresh_ms
            .is_some_and(|last| now.saturating_sub(last) < 60_000)
        {
            return Err("来源刷新至少间隔一分钟，请稍后重试".into());
        }
        state.blocked = false;
        state.next_fetch = 0;
        state.status.source_state = "waiting".into();
        let status = state.status.clone();
        drop(state);
        self.wake.notify_one();
        Ok(status)
    }

    /// Runs independently of watcher rounds and source downloads. Eviction is
    /// already applied by the client before it tries the next slot.
    async fn maintain(&self) -> Option<PoolStatus> {
        let mut state = self.inner.lock().await;
        if !state.status.enabled {
            return None;
        }
        let candidates = state.candidates.clone();
        if self
            .client
            .replenish_automatic_pool(candidates)
            .await
            .is_err()
        {
            state.status.message = "候选线路装配失败，保留其余线路，稍后补位。".into();
        }
        let entries = self
            .client
            .automatic_pool_entries()
            .await
            .unwrap_or_default();
        if entries != state.status.entries {
            state.status.entries = entries;
            return Some(state.status.clone());
        }
        None
    }

    async fn maintain_and_emit(&self, app: &AppHandle) {
        if let Some(status) = self.maintain().await {
            let _ = app.emit(EVENT, &status);
        }
    }

    pub async fn run(self, app: AppHandle) {
        loop {
            self.maintain_and_emit(&app).await;
            let job = {
                let mut state = self.inner.lock().await;
                if state.status.enabled && !state.blocked && now_ms() >= state.next_fetch {
                    state.next_fetch = now_ms().saturating_add(REFRESH_MS);
                    state.status.source_state = "fetching".into();
                    let _ = app.emit(EVENT, &state.status);
                    Some((state.generation, state.status.source.clone()))
                } else {
                    None
                }
            };
            if let Some((generation, source)) = job {
                let fetch = async {
                    if source == "proxy5Browser" {
                        let profile = self.profile.clone();
                        match tokio::task::spawn_blocking(move || read_browser(profile)).await {
                            Ok(result) => result,
                            Err(_) => Err("浏览器来源任务失败".into()),
                        }
                    } else {
                        fetch_public_feeds()
                            .await
                            .map(|(candidates, message)| SourceResult {
                                state: "ready".into(),
                                message,
                                candidates,
                            })
                    }
                };
                tokio::pin!(fetch);
                // A slow subscription/browser refresh must not delay cached
                // replacements while stock queries try the remaining slots.
                let result = loop {
                    tokio::select! {
                        result = &mut fetch => break result,
                        _ = self.client.automatic_pool_changed() => self.maintain_and_emit(&app).await,
                        _ = self.wake.notified() => self.maintain_and_emit(&app).await,
                    }
                };
                let mut state = self.inner.lock().await;
                if state.generation == generation && state.status.enabled {
                    state.status.last_refresh_ms = Some(now_ms());
                    match result {
                        Ok(result) => {
                            state.blocked = result.state == "blocked";
                            state.status.source_state = result.state;
                            state.status.message = result.message;
                            state.candidates = result.candidates;
                            let candidates = state.candidates.clone();
                            if self
                                .client
                                .replenish_automatic_pool(candidates)
                                .await
                                .is_err()
                            {
                                state.status.source_state = "error".into();
                                state.status.message = "候选线路装配失败，保留其余线路。".into();
                            }
                            state.status.entries = self
                                .client
                                .automatic_pool_entries()
                                .await
                                .unwrap_or_default();
                        }
                        Err(message) => {
                            state.status.source_state = "error".into();
                            state.status.message = message;
                            // Existing routes survive temporary source failures.
                        }
                    }
                    let _ = app.emit(EVENT, &state.status);
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(15)) => {},
                _ = self.wake.notified() => {},
                _ = self.client.automatic_pool_changed() => {},
            }
        }
    }
}

#[derive(Deserialize)]
struct SourceResult {
    state: String,
    message: String,
    candidates: Vec<Candidate>,
}

fn read_browser(profile: PathBuf) -> Result<SourceResult, String> {
    let script = concat!(
        include_str!("proxy5-browser.mjs"),
        "\nrun().then(r=>console.log(JSON.stringify(r))).catch(()=>{console.log(JSON.stringify({state:'error',message:'浏览器来源不可用；需要Chrome和Node.js 22+，请人工检查专用窗口。',candidates:[]}));process.exitCode=1;});"
    );
    let mut child = Command::new("node")
        .args(["--input-type=module", "-e", script])
        .env("APW_PROXY5_PROFILE", profile)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| "未找到Node.js 22+；公开订阅源模式不需要Node.js。".to_string())?;
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(100)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return Err("浏览器读取超时，已停止本次读取；不会绕过验证。".into());
            }
        }
    }
    let output = child.wait_with_output().map_err(|_| "浏览器输出读取失败")?;
    if output.stdout.len() > 64 * 1024 {
        return Err("浏览器返回内容过大".into());
    }
    serde_json::from_slice(&output.stdout).map_err(|_| "浏览器未返回有效候选结果".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service(enabled: bool) -> PoolService {
        PoolService::new(
            AppleClient::new(Default::default()).unwrap(),
            enabled,
            "publicFeeds".into(),
            PathBuf::from("unused-offline-fixture"),
        )
    }

    #[tokio::test]
    async fn explicit_refresh_needs_enabled_and_is_bounded() {
        let disabled = service(false);
        assert!(disabled.refresh().await.is_err());
        let enabled = service(true);
        {
            let mut state = enabled.inner.lock().await;
            state.status.source_state = "fetching".into();
        }
        assert!(enabled.refresh().await.is_err());
        {
            let mut state = enabled.inner.lock().await;
            state.status.source_state = "blocked".into();
            state.status.last_refresh_ms = Some(now_ms());
            state.blocked = true;
        }
        assert!(enabled.refresh().await.is_err());
        {
            let mut state = enabled.inner.lock().await;
            state.status.last_refresh_ms = Some(now_ms() - 61_000);
        }
        assert_eq!(enabled.refresh().await.unwrap().source_state, "waiting");
        assert!(!enabled.inner.lock().await.blocked);
        assert!(enabled.status().await.entries.is_empty());
    }

    #[tokio::test]
    async fn cached_candidates_fill_slots_even_while_source_is_fetching_or_blocked() {
        let service = service(true);
        service.client.enable_automatic_pool().await;
        {
            let mut state = service.inner.lock().await;
            state.status.source_state = "fetching".into();
            state.candidates = (1..=6)
                .map(|n| Candidate {
                    url: format!("http://8.8.8.{n}:8080"),
                    source: "offline-no-connect".into(),
                    uptime: None,
                    latency_ms: None,
                })
                .collect();
        }
        let status = service
            .maintain()
            .await
            .expect("cached candidates installed without source IO");
        assert_eq!(status.source_state, "fetching");
        assert_eq!(status.entries.len(), 5);
        assert_eq!(
            status.entries.iter().map(|e| e.slot).collect::<Vec<_>>(),
            [0, 1, 2, 3, 4]
        );
        assert!(
            service.maintain().await.is_none(),
            "unchanged status is not re-emitted"
        );
        service.client.enable_automatic_pool().await;
        {
            let mut state = service.inner.lock().await;
            state.status.source_state = "blocked".into();
            state.blocked = true;
            state.status.entries.clear();
        }
        assert_eq!(service.maintain().await.unwrap().entries.len(), 5);
        assert!(
            service.inner.lock().await.blocked,
            "cache maintenance must not restart a blocked source"
        );
    }
}
