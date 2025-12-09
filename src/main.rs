use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};
use tracing::{error, info, warn};
use url::Url;

#[derive(Parser, Debug)]
struct Args {
    /// Address to listen on for local clients, e.g. 127.0.0.1:9090
    #[arg(long, short('s'), default_value_t = SocketAddr::from(([127, 0, 0, 1], 9090)))]
    listen: SocketAddr,
    /// Upstream Sei websocket RPC URL, e.g. https://evm-rpc-testnet.sei-apis.com
    #[arg(long, short('u'))]
    upstream: String,
}

/// Map http(s) → ws(s) and validate scheme.
fn normalize_upstream(u: &str) -> anyhow::Result<Url> {
    let mut url = Url::parse(u)?;

    match url.scheme() {
        "ws" | "wss" => {}
        "http" => {
            url.set_scheme("ws")
                .map_err(|_| anyhow::anyhow!("failed to change scheme http→ws"))?;
        }
        "https" => {
            url.set_scheme("wss")
                .map_err(|_| anyhow::anyhow!("failed to change scheme https→wss"))?;
        }
        s => return Err(anyhow::anyhow!("unsupported upstream scheme: {s}")),
    }

    Ok(url)
}

/// Drop only invalid *subscription notifications*:
/// JSON-RPC object with method == "eth_subscription" whose params.result
/// is an object missing the required "address" field (logs subscription case).
fn should_drop_upstream_text(txt: &str) -> bool {
    let v: Value = match serde_json::from_str(txt) {
        Ok(v) => v,
        Err(_) => return false, // not JSON => forward
    };
    let obj = match v.as_object() {
        Some(o) => o,
        None => return false, // not an object => forward
    };
    if obj.get("method").and_then(|m| m.as_str()) != Some("eth_subscription") {
        return false; // only filter subscription notifications
    }

    let result = obj.get("params").and_then(|p| p.get("result"));

    match result {
        Some(Value::Object(map)) => !map.contains_key("address"),
        _ => false, // no result / non-object result => forward
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("install rustls crypto provider");

    // Normalize once.
    let upstream_url = normalize_upstream(&args.upstream)?;
    info!("normalized upstream = {}", upstream_url);

    // Test connect once at startup.
    info!("testing upstream connection...");
    match connect_async(upstream_url.as_str()).await {
        Ok((mut ws, _)) => {
            info!("upstream connection OK");
            let _ = ws.close(None).await; // best-effort clean close
        }
        Err(e) => {
            error!("upstream connection test failed: {e}");
            return Err(e.into());
        }
    }

    let listener = TcpListener::bind(args.listen).await?;
    info!("listening on {}", args.listen);

    let upstream_url = Arc::new(upstream_url.to_string());

    loop {
        let (stream, peer) = listener.accept().await?;
        let upstream_url = upstream_url.clone();

        tokio::spawn(async move {
            info!("client connected: {}", peer);

            let client_ws = match accept_async(stream).await {
                Ok(ws) => ws,
                Err(e) => {
                    error!("accept ws failed: {e}");
                    return;
                }
            };

            let upstream_ws = match connect_async(upstream_url.as_str()).await {
                Ok((ws, _)) => ws,
                Err(e) => {
                    error!("connect upstream failed: {e}");
                    return;
                }
            };

            let (mut c_tx, mut c_rx) = client_ws.split();
            let (mut u_tx, mut u_rx) = upstream_ws.split();

            let c2u = async {
                while let Some(msg) = c_rx.next().await {
                    match msg {
                        Ok(m) => {
                            if let Err(e) = u_tx.send(m).await {
                                error!("send to upstream failed: {e}");
                                break;
                            }
                        }
                        Err(e) => {
                            error!("client recv failed: {e}");
                            break;
                        }
                    }
                }
            };

            let u2c = async {
                while let Some(msg) = u_rx.next().await {
                    match msg {
                        Ok(Message::Text(txt)) => {
                            if should_drop_upstream_text(&txt) {
                                warn!("dropped invalid eth_subscription message: {}", txt);
                                continue;
                            }
                            if let Err(e) = c_tx.send(Message::Text(txt)).await {
                                error!("send to client failed: {e}");
                                break;
                            }
                        }
                        Ok(other) => {
                            if let Err(e) = c_tx.send(other).await {
                                error!("send to client failed: {e}");
                                break;
                            }
                        }
                        Err(e) => {
                            error!("upstream recv failed: {e}");
                            break;
                        }
                    }
                }
            };

            tokio::select! {
                _ = c2u => {},
                _ = u2c => {},
            }

            info!("client disconnected: {}", peer);
        });
    }
}
