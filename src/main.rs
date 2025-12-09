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
    // Relay everything from client -> upstream, but log close/control/error with detail.
    while let Some(msg) = c_rx.next().await {
        match msg {
            Ok(m) => {
                match &m {
                    Message::Close(frame_opt) => {
                        // Client initiated close: log full close info.
                        if let Some(frame) = frame_opt {
                            warn!(
                                %peer,
                                code = ?frame.code,
                                reason = %frame.reason,
                                "client sent close frame"
                            );
                        } else {
                            warn!(%peer, "client sent close frame (no payload)");
                        }
                    }
                    // Message::Ping(_) => {
                    //     info!(%peer, "client ping");
                    // }
                    // Message::Pong(_) => {
                    //     info!(%peer, "client pong");
                    // }
                    _ => {}
                }

                if let Err(e) = u_tx.send(m).await {
                    error!(%peer, "send to upstream failed: {e}");
                    return Err(e.into());
                }
            }
            Err(e) => {
                // Classify tungstenite errors so you see real cause.
                use tokio_tungstenite::tungstenite::Error as WsErr;
                match &e {
                    WsErr::ConnectionClosed => {
                        warn!(%peer, "client recv: connection closed cleanly");
                    }
                    WsErr::AlreadyClosed => {
                        warn!(%peer, "client recv: already closed");
                    }
                    WsErr::Io(ioe) => {
                        error!(%peer, io_error = %ioe, "client recv: IO error");
                    }
                    WsErr::Protocol(pe) => {
                        error!(%peer, protocol_error = %pe, "client recv: protocol error");
                    }
                    WsErr::Utf8(e) => {
                        error!(%peer, err=%e, "client recv: invalid utf8");
                    }
                    other => {
                        error!(%peer, err = %other, "client recv: websocket error");
                    }
                }
                return Err(e.into());
            }
        }
    }

    // Stream ended without an error (EOF) — this is a common “looks like a drop” case.
    warn!(%peer, "client stream ended (EOF / None from c_rx)");
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
    // Relay upstream -> client, filtering only invalid subscription notifications.
    while let Some(msg) = u_rx.next().await {
        match msg {
            Ok(m) => {
                match &m {
                    Message::Close(frame_opt) => {
                        // Upstream initiated close: log full close info.
                        if let Some(frame) = frame_opt {
                            warn!(
                                %peer,
                                code = ?frame.code,
                                reason = %frame.reason,
                                "upstream sent close frame"
                            );
                        } else {
                            warn!(%peer, "upstream sent close frame (no payload)");
                        }
                    }
                    // `                    Message::Ping(_) => {
                    //                         info!(%peer, "upstream ping");
                    //                     }
                    //                     Message::Pong(_) => {
                    //                         info!(%peer, "upstream pong");
                    //                     }`
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
                // Classify tungstenite errors so you see real cause.
                use tokio_tungstenite::tungstenite::Error as WsErr;
                match &e {
                    WsErr::ConnectionClosed => {
                        warn!(%peer, "upstream recv: connection closed cleanly");
                    }
                    WsErr::AlreadyClosed => {
                        warn!(%peer, "upstream recv: already closed");
                    }
                    WsErr::Io(ioe) => {
                        error!(%peer, io_error = %ioe, "upstream recv: IO error");
                    }
                    WsErr::Protocol(pe) => {
                        error!(%peer, protocol_error = %pe, "upstream recv: protocol error");
                    }
                    WsErr::Utf8(e) => {
                        error!(%peer, err=%e, "upstream recv: invalid utf8");
                    }
                    other => {
                        error!(%peer, err = %other, "upstream recv: websocket error");
                    }
                }
                return Err(e.into());
            }
        }
    }

    // Stream ended without an error (EOF).
    warn!(%peer, "upstream stream ended (EOF / None from u_rx)");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("install rustls crypto provider");

    tracing_subscriber::fmt::init();
    let args = Args::parse();

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

            // Log which direction ended first, and with what.
            tokio::select! {
                res = c2u => {
                    match res {
                        Ok(()) => warn!(%peer, "c2u ended cleanly (client->upstream)"),
                        Err(e) => warn!(%peer, "c2u ended with error: {e}"),
                    }
                }
                res = u2c => {
                    match res {
                        Ok(()) => warn!(%peer, "u2c ended cleanly (upstream->client)"),
                        Err(e) => warn!(%peer, "u2c ended with error: {e}"),
                    }
                }
            }

            info!(%peer, "client disconnected");
        });
    }
}
