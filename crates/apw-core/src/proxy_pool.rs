//! Bounded candidate pool. No network IO or inventory decisions here.
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolEntry {
    /// Stable zero-based A..E position. Removing B never moves C into B.
    pub slot: usize,
    pub url: String,
    pub source: String,
    /// A response is not evidence of available stock.
    pub responded: bool,
}

#[derive(Clone, Default, Debug)]
pub struct ProxyPool {
    entries: Vec<PoolEntry>,
    rejected: HashMap<String, u64>,
}

impl ProxyPool {
    pub fn entries(&self) -> &[PoolEntry] {
        &self.entries
    }

    /// Fill vacant slots only, preserving the order and state of other entries.
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
            let Some(slot) =
                (0..TARGET_SIZE).find(|slot| !self.entries.iter().any(|e| e.slot == *slot))
            else {
                break;
            };
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
                slot,
                url: candidate.url,
                source: candidate.source,
                responded: false,
            });
        }
        self.entries.sort_by_key(|e| e.slot);
    }

    /// A single actual failed query retires its endpoint. Local waits never call this.
    pub fn evict(&mut self, url: &str, now_ms: u64) -> bool {
        let before = self.entries.len();
        self.entries.retain(|entry| entry.url != url);
        if self.entries.len() == before {
            return false;
        }
        self.rejected
            .insert(url.to_string(), now_ms.saturating_add(REJECT_FOR_MS));
        true
    }

    pub fn mark_responded(&mut self, url: &str) {
        if let Some(entry) = self.entries.iter_mut().find(|e| e.url == url) {
            entry.responded = true;
        }
    }

    #[cfg(test)]
    pub(crate) fn fixture(urls: &[String]) -> Self {
        assert!(urls.len() <= TARGET_SIZE);
        Self {
            entries: urls
                .iter()
                .enumerate()
                .map(|(slot, url)| PoolEntry {
                    slot,
                    url: url.clone(),
                    source: "offline-fixture".into(),
                    responded: false,
                })
                .collect(),
            rejected: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(n: u8) -> Candidate {
        Candidate {
            url: format!("http://8.8.8.{n}:8080"),
            source: "fixture".into(),
            uptime: None,
            latency_ms: Some(f64::from(n)),
        }
    }

    #[test]
    fn replacements_keep_the_vacant_slot_and_other_states() {
        let mut pool = ProxyPool::default();
        pool.replenish((1..=9).map(candidate).collect(), 0);
        assert_eq!(
            pool.entries().iter().map(|e| e.slot).collect::<Vec<_>>(),
            [0, 1, 2, 3, 4]
        );
        let old = pool.entries()[1].url.clone();
        pool.mark_responded(&pool.entries()[2].url.clone());
        let c = pool.entries()[2].clone();
        assert!(pool.evict(&old, 1));
        assert!(!pool.evict(&old, 1));
        assert_eq!(pool.entries()[1], c);
        pool.replenish((1..=9).map(candidate).collect(), 2);
        assert_eq!(pool.entries()[1].slot, 1);
        assert_eq!(pool.entries()[1].url, candidate(6).url);
        assert!(!pool.entries()[1].responded);
        assert_eq!(pool.entries()[2], c);
        let snapshot = pool.entries().to_vec();
        pool.replenish((10..=20).map(candidate).collect(), 3);
        assert_eq!(pool.entries(), snapshot);
    }

    #[test]
    fn rejected_endpoint_stays_out_for_thirty_minutes() {
        let mut pool = ProxyPool::default();
        pool.replenish(vec![candidate(1)], 0);
        pool.evict(&candidate(1).url, 100);
        pool.replenish(vec![candidate(1)], REJECT_FOR_MS + 99);
        assert!(pool.entries().is_empty());
        pool.replenish(vec![candidate(1)], REJECT_FOR_MS + 100);
        assert_eq!(pool.entries().len(), 1);
    }

    #[test]
    fn ordering_deduplication_and_invalid_metrics_are_preserved() {
        let mut candidates = vec![candidate(1), candidate(2), candidate(3), candidate(4)];
        candidates[1].uptime = Some(99.0);
        candidates[2].latency_ms = Some(f64::NAN);
        candidates[3].uptime = Some(101.0);
        candidates.push(Candidate {
            url: "socks5h://8.8.8.2:1080".into(),
            ..candidate(2)
        });
        candidates.push(Candidate {
            uptime: Some(f64::INFINITY),
            ..candidate(5)
        });
        candidates.push(Candidate {
            latency_ms: Some(-1.0),
            ..candidate(6)
        });
        let mut pool = ProxyPool::default();
        pool.replenish(candidates, 0);
        assert_eq!(pool.entries().len(), 2);
        assert_eq!(pool.entries()[0].url, candidate(2).url);
        assert_eq!(pool.entries()[1].url, candidate(1).url);
    }

    #[test]
    fn untrusted_candidates_cannot_inject_credentials_or_local_endpoints() {
        for raw in [
            "http://127.0.0.1:80",
            "http://10.1.1.1:80",
            "http://192.168.1.1:80",
            "http://172.16.1.1:80",
            "http://169.254.169.254:80",
            "http://100.64.1.1:80",
            "http://198.18.1.1:80",
            "http://203.0.113.1:80",
            "http://224.1.1.1:80",
            "http://[::1]:80",
            "http://[fc00::1]:80",
            "http://[2001:db8::1]:80",
            "http://[::ffff:127.0.0.1]:80",
            "http://example.com:80",
            "http://user:pass@8.8.8.8:80",
            "http://8.8.8.8:80/path",
            "http://8.8.8.8:80/?secret=1",
            "http://8.8.8.8:80/#fragment",
            "ftp://8.8.8.8:80",
            "http://8.8.8.8:0",
        ] {
            assert!(!valid_public_proxy(raw), "must reject {raw}");
        }
        for raw in [
            "http://8.8.8.8:8080",
            "https://1.1.1.1:443",
            "socks5://9.9.9.9:1080",
            "socks5h://[2606:4700:4700::1111]:1080",
        ] {
            assert!(valid_public_proxy(raw), "must accept {raw}");
        }
        let mut pool = ProxyPool::default();
        pool.replenish(
            vec![Candidate {
                url: "http://localhost:8080".into(),
                ..candidate(1)
            }],
            0,
        );
        assert!(pool.entries().is_empty());
    }
}
