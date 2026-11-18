//! Public, fixed-origin feeds. Never disable TLS validation or probe endpoints.
use crate::proxy_pool::{Candidate, valid_public_proxy};
use serde_json::Value;
use std::time::Duration;

const MAX_BYTES: usize = 4 << 20;
const MAX_CANDIDATES: usize = 2000;
const SOURCES: [(&str, &str); 2] = [
    (
        "monosans",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies.json",
    ),
    (
        "proxifly",
        "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/all/data.json",
    ),
];

pub fn parse_feed(name: &str, data: &[u8]) -> Result<Vec<Candidate>, String> {
    let rows: Vec<Value> =
        serde_json::from_slice(data).map_err(|_| "来源不是有效 JSON 数组".to_string())?;
    let mut out = Vec::new();
    for row in rows {
        let (url, latency_ms) = match name {
            "monosans" => {
                if row.get("username").is_some_and(|v| !v.is_null())
                    || row.get("password").is_some_and(|v| !v.is_null())
                {
                    continue;
                }
                let (Some(protocol), Some(host), Some(port)) = (
                    row["protocol"].as_str(),
                    row["host"].as_str(),
                    row["port"].as_u64(),
                ) else {
                    continue;
                };
                if port == 0 || port > 65535 {
                    continue;
                }
                let protocol = match protocol {
                    "socks5" => "socks5h",
                    "http" => "http",
                    _ => continue,
                };
                let host = if host.contains(':') {
                    format!("[{host}]")
                } else {
                    host.to_string()
                };
                (
                    format!("{protocol}://{host}:{port}"),
                    row["timeout"].as_f64().map(|v| v * 1000.0),
                )
            }
            "proxifly" => {
                let Some(url) = row["proxy"].as_str() else {
                    continue;
                };
                (url.to_owned(), None)
            }
            _ => return Err("不支持的公开来源".into()),
        };
        if !valid_public_proxy(&url) || latency_ms.is_some_and(|v| !v.is_finite() || v < 0.0) {
            continue;
        }
        out.push(Candidate {
            url,
            source: name.into(),
            uptime: None,
            latency_ms,
        });
        if out.len() >= MAX_CANDIDATES {
            break;
        }
    }
    Ok(out)
}

pub async fn fetch_public_feeds() -> Result<(Vec<Candidate>, String), String> {
    // A separate direct client: do not leak notification credentials or use
    // candidate proxies to download the provider list itself.
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| "无法创建来源客户端")?;
    let mut candidates = Vec::new();
    let mut notices = Vec::new();
    for (name, url) in SOURCES {
        let result = async {
            let mut response = http
                .get(url)
                .send()
                .await
                .map_err(|_| "来源连接失败".to_string())?;
            if !response.status().is_success() {
                return Err(format!("来源 HTTP {}", response.status().as_u16()));
            }
            if response
                .content_length()
                .is_some_and(|n| n > MAX_BYTES as u64)
            {
                return Err("来源内容过大".into());
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| "来源读取失败".to_string())?
            {
                if bytes.len() + chunk.len() > MAX_BYTES {
                    return Err("来源内容过大".into());
                }
                bytes.extend_from_slice(&chunk);
            }
            parse_feed(name, &bytes)
        }
        .await;
        match result {
            Ok(rows) => {
                notices.push(format!("{name}: {} 个候选", rows.len()));
                candidates.extend(rows);
            }
            Err(reason) => notices.push(format!("{name}: {reason}")),
        }
    }
    if candidates.is_empty() {
        return Err(notices.join("；"));
    }
    Ok((
        candidates,
        format!(
            "{}。来源检测不等于本机可用；替代源没有90%在线率保证。",
            notices.join("；")
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_without_copying_auth_or_geo_data() {
        let rows = br#"[{"protocol":"socks5","host":"8.8.8.8","port":1080,"timeout":0.2}, {"protocol":"http","host":"127.0.0.1","port":80}, {"protocol":"http","host":"8.8.4.4","port":80,"password":"secret"}]"#;
        let got = parse_feed("monosans", rows).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].url, "socks5h://8.8.8.8:1080");
        assert_eq!(got[0].latency_ms, Some(200.0));
        assert!(got[0].uptime.is_none());
    }
    #[test]
    fn rejects_invalid_shape_and_unsupported_protocols() {
        assert!(parse_feed("monosans", br#"{}"#).is_err());
        let rows = br#"[{"proxy":"socks4://8.8.8.8:1080"},{"proxy":"http://8.8.4.4:8080"}]"#;
        let got = parse_feed("proxifly", rows).unwrap();
        assert_eq!(got.len(), 1);
        assert!(got[0].latency_ms.is_none());
    }
}
