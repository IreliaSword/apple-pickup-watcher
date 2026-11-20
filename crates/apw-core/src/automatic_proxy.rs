//! Automatic-mode routing only. Manual proxies keep their existing policy.
use super::*;
use crate::proxy_pool::{Candidate, PoolEntry, ProxyPool, TARGET_SIZE};

#[derive(Debug)]
pub(super) struct AutomaticState {
    generation: usize,
    pool: ProxyPool,
    next_slot: usize,
}

impl AutomaticState {
    pub(super) fn new(generation: usize) -> Self {
        Self {
            generation,
            pool: ProxyPool::default(),
            next_slot: 0,
        }
    }
}

impl AppleClient {
    /// Start a fresh automatic generation, atomically removing all direct routes.
    pub async fn enable_automatic_pool(&self) {
        let mut automatic = self.automatic.lock().await;
        let mut routes = self.routes.write().await;
        routes.clear();
        let generation = self.automatic_generation.fetch_add(1, Ordering::Relaxed) + 1;
        *automatic = Some(AutomaticState::new(generation));
        self.automatic_changed.notify_one();
    }

    pub async fn automatic_pool_entries(&self) -> Option<Vec<PoolEntry>> {
        self.automatic
            .lock()
            .await
            .as_ref()
            .map(|s| s.pool.entries().to_vec())
    }

    /// Wakes the desktop maintainer immediately after success or eviction.
    pub async fn automatic_pool_changed(&self) {
        self.automatic_changed.notified().await;
    }

    /// No source IO here. The desktop task supplies its cached or downloaded candidates.
    pub async fn replenish_automatic_pool(
        &self,
        candidates: Vec<Candidate>,
    ) -> Result<Vec<PoolEntry>, ApiError> {
        let mut automatic = self.automatic.lock().await;
        let Some(state) = automatic.as_mut() else {
            return Ok(vec![]);
        };
        let mut proposed = state.pool.clone();
        proposed.replenish(candidates, now_ms());
        if proposed.entries() != state.pool.entries() {
            self.install_automatic_routes(&proposed).await?;
            state.pool = proposed;
        }
        Ok(state.pool.entries().to_vec())
    }

    // Caller holds automatic before routes. Reuse retained Arc<Route> sessions.
    async fn install_automatic_routes(&self, pool: &ProxyPool) -> Result<(), ApiError> {
        let mut routes = self.routes.write().await;
        let mut next = Vec::new();
        for entry in pool.entries() {
            let retained = routes
                .iter()
                .find(|r| r.proxy_url.as_deref() == Some(entry.url.as_str()));
            let route = match retained {
                Some(route) => Arc::clone(route),
                None => Arc::new(Route::build(
                    &self.config,
                    format!("proxy#{}", self.next_route.fetch_add(1, Ordering::Relaxed)),
                    Some(&entry.url),
                )?),
            };
            next.push(route);
        }
        *routes = next;
        Ok(())
    }

    async fn pick_automatic_route(
        &self,
        region: &Region,
        generation: usize,
        tried: &[usize],
    ) -> Result<(Arc<Route>, usize, bool), ApiError> {
        let mut automatic = self.automatic.lock().await;
        let Some(state) = automatic.as_mut().filter(|s| s.generation == generation) else {
            return Err(ApiError::Blocked("代理模式已变更，等待下一轮查询".into()));
        };
        let routes = self.routes.read().await;
        for offset in 0..TARGET_SIZE {
            let slot = (state.next_slot + offset) % TARGET_SIZE;
            if tried.contains(&slot) {
                continue;
            }
            let Some(entry) = state.pool.entries().iter().find(|e| e.slot == slot) else {
                continue;
            };
            let Some(route) = routes
                .iter()
                .find(|r| r.proxy_url.as_deref() == Some(entry.url.as_str()))
            else {
                continue;
            };
            if let Ok(probing) = route
                .blocks
                .lock()
                .await
                .admit(region.locale, Instant::now())
            {
                state.next_slot = (slot + 1) % TARGET_SIZE;
                return Ok((Arc::clone(route), slot, probing));
            }
        }
        Err(ApiError::Blocked(
            "自动代理本轮已无可用槽位，等待后台补位并退避；不会回退直连".into(),
        ))
    }

    async fn finish_automatic_attempt(&self, generation: usize, route: &Arc<Route>, success: bool) {
        let mut automatic = self.automatic.lock().await;
        let Some(state) = automatic.as_mut().filter(|s| s.generation == generation) else {
            return;
        };
        let mut routes = self.routes.write().await;
        if !routes.iter().any(|r| Arc::ptr_eq(r, route)) {
            return;
        }
        let Some(url) = route.proxy_url.as_deref() else {
            return;
        };
        if success {
            state.pool.mark_responded(url);
        } else {
            state.pool.evict(url, now_ms());
            routes.retain(|r| !Arc::ptr_eq(r, route));
        }
        self.automatic_changed.notify_one();
    }

    /// One logical stock query, at most one actual pickup attempt per A..E slot.
    /// The watcher sleeps its normal interval only after this call has returned.
    pub(super) async fn query_automatic<T: Send, F>(
        &self,
        region: &Region,
        scope: PickupScope,
        parts: &[String],
        parse: F,
    ) -> Result<T, ApiError>
    where
        F: Fn(&[u8]) -> Result<T, ApiError> + Send + Sync,
    {
        if parts.is_empty() {
            return Err(ApiError::Transport("零件号列表为空".into()));
        }
        // Prevent parallel store groups from overtaking a failed slot's fallback.
        let _serial = self.automatic_query.lock().await;
        let generation = self
            .automatic
            .lock()
            .await
            .as_ref()
            .map(|s| s.generation)
            .ok_or_else(|| ApiError::Blocked("代理模式已变更，等待下一轮查询".into()))?;
        let mut query = vec![
            ("pl".into(), "true".into()),
            ("mts.0".into(), "regular".into()),
            scope.query_param(),
        ];
        query.extend(
            parts
                .iter()
                .enumerate()
                .map(|(i, part)| (format!("parts.{i}"), part.clone())),
        );
        let url = region.pickup_message_url();
        let referer = region.default_buy_page_url();
        let outbound = Outbound {
            url: &url,
            query: &query,
            region,
            referer: &referer,
            scope: &scope,
            parts: parts.len(),
        };
        let mut tried = Vec::new();
        let mut last_error = None;
        let mut outcomes = Vec::new();
        for _ in 0..TARGET_SIZE {
            let (route, slot, probing) =
                match self.pick_automatic_route(region, generation, &tried).await {
                    Ok(route) => route,
                    Err(err) => return Err(summarize_round(last_error.unwrap_or(err), &outcomes)),
                };
            tried.push(slot);
            self.ensure_warm(&route, region).await;
            // Keep the global minimum interval and each route's budget, but no
            // same-route exponential retries or user polling interval on failure.
            self.throttle(&route).await;
            if self.automatic_generation.load(Ordering::Relaxed) != generation {
                if probing {
                    self.end_probe(&route, region).await;
                }
                return Err(ApiError::Blocked("代理模式已变更，取消旧线路查询".into()));
            }
            let result = self
                .get_once(&route, &outbound)
                .await
                .and_then(|body| parse(&body));
            if result.is_ok() {
                self.clear_block(&route, region, probing).await;
            } else if probing {
                self.end_probe(&route, region).await;
            }
            self.finish_automatic_attempt(generation, &route, result.is_ok())
                .await;
            match result {
                Ok(value) => return Ok(value),
                Err(err) => {
                    outcomes.push(format!(
                        "槽位 {}（{}）查询失败，已淘汰并通知后台补位：{err}",
                        char::from(b'A' + slot as u8),
                        route.label
                    ));
                    last_error = Some(err);
                }
            }
        }
        Err(summarize_round(
            last_error.unwrap_or_else(|| ApiError::Blocked("自动代理池暂无可用线路".into())),
            &outcomes,
        ))
    }
}

/// Unknown or missing requested results are never mistaken for a successful check.
/// Legitimate no-stock and pickup-not-open responses are still successful queries.
pub(super) fn validate_result(
    stores: &[StoreAvailability],
    parts: &[String],
) -> Result<(), ApiError> {
    for part in parts {
        let mut found = false;
        for store in stores {
            if let Some(status) = store.parts.get(part) {
                found = true;
                if status.availability.is_failure() {
                    return Err(ApiError::SchemaDrift {
                        field: "partsAvailability".into(),
                        raw: "请求型号的库存结果未知".into(),
                    });
                }
            }
        }
        if !found {
            return Err(ApiError::SchemaDrift {
                field: "partsAvailability".into(),
                raw: "响应缺少请求型号".into(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "automatic_proxy_tests.rs"]
mod tests;
