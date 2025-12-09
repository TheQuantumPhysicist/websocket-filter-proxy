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
    let Some(Value::Object(map)) = result else {
        return false; // no/odd result => forward
    };

    // 1) Drop truly empty results like {}
    if map.is_empty() {
        return true;
    }

    // 2) If it *looks like a log* but lacks required "address", drop.
    let log_like = map.contains_key("topics")
        || map.contains_key("data")
        || map.contains_key("logIndex")
        || map.contains_key("transactionHash")
        || map.contains_key("removed");

    log_like && !map.contains_key("address")
}

async fn pump_client_to_upstream<
    C: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    U: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
>(
    peer: SocketAddr,
    mut c_rx: C,
    mut u_tx: U,
) -> anyhow::Result<()> {
    while let Some(msg) = c_rx.next().await {
        match msg {
            Ok(m) => {
                match &m {
                    Message::Close(frame) => {
                        warn!(%peer, ?frame, "client sent close");
                    }
                    Message::Ping(_) => {
                        info!(%peer, "client ping");
                    }
                    Message::Pong(_) => {
                        info!(%peer, "client pong");
                    }
                    _ => {}
                }
                if let Err(e) = u_tx.send(m).await {
                    error!(%peer, "send to upstream failed: {e}");
                    return Err(e.into());
                }
            }
            Err(e) => {
                error!(%peer, "client recv failed: {e}");
                return Err(e.into());
            }
        }
    }
    warn!(%peer, "client stream ended (EOF)");
    Ok(())
}

async fn pump_upstream_to_client<
    U: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    C: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
>(
    peer: SocketAddr,
    mut u_rx: U,
    mut c_tx: C,
) -> anyhow::Result<()> {
    while let Some(msg) = u_rx.next().await {
        match msg {
            Ok(m) => {
                match &m {
                    Message::Close(frame) => {
                        warn!(%peer, ?frame, "upstream sent close");
                    }
                    Message::Ping(_) => {
                        info!(%peer, "upstream ping");
                    }
                    Message::Pong(_) => {
                        info!(%peer, "upstream pong");
                    }
                    Message::Text(txt) => {
                        if should_drop_upstream_text(txt) {
                            warn!(%peer, "dropped invalid eth_subscription message: {}", txt);
                            continue;
                        }
                    }
                    _ => {}
                }

                if let Err(e) = c_tx.send(m).await {
                    error!(%peer, "send to client failed: {e}");
                    return Err(e.into());
                }
            }
            Err(e) => {
                error!(%peer, "upstream recv failed: {e}");
                return Err(e.into());
            }
        }
    }
    warn!(%peer, "upstream stream ended (EOF)");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("install rustls crypto provider");

    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let upstream_url = normalize_upstream(&args.upstream)?;
    info!("normalized upstream = {}", upstream_url);

    info!("testing upstream connection...");
    match connect_async(upstream_url.as_str()).await {
        Ok((mut ws, _)) => {
            info!("upstream connection OK");
            let _ = ws.close(None).await;
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
            info!(%peer, "client connected");

            let client_ws = match accept_async(stream).await {
                Ok(ws) => ws,
                Err(e) => {
                    error!(%peer, "accept ws failed: {e}");
                    return;
                }
            };

            let upstream_ws = match connect_async(upstream_url.as_str()).await {
                Ok((ws, _)) => ws,
                Err(e) => {
                    error!(%peer, "connect upstream failed: {e}");
                    return;
                }
            };

            let (c_tx, c_rx) = client_ws.split();
            let (u_tx, u_rx) = upstream_ws.split();

            let c2u = pump_client_to_upstream(peer, c_rx, u_tx);
            let u2c = pump_upstream_to_client(peer, u_rx, c_tx);

            tokio::select! {
                res = c2u => {
                    if let Err(e) = res {
                        warn!(%peer, "c2u ended with error: {e}");
                    } else {
                        info!(%peer, "c2u ended cleanly");
                    }
                }
                res = u2c => {
                    if let Err(e) = res {
                        warn!(%peer, "u2c ended with error: {e}");
                    } else {
                        info!(%peer, "u2c ended cleanly");
                    }
                }
            }

            info!(%peer, "client disconnected");
        });
    }
}
