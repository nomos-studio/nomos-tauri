use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tauri::Manager;
use tokio::sync::mpsc;
use tokio::time;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use futures_util::{SinkExt, StreamExt};

// ── Keyboard WebSocket state ──────────────────────────────────────────────

struct KeyboardWs {
    tx: mpsc::UnboundedSender<String>,
    ref_seq: AtomicU64,
}

// ── Tauri command: called by the JS keydown/keyup shim in root.html.heex ──

#[tauri::command]
fn key_event(key: String, pressed: bool, state: tauri::State<'_, KeyboardWs>) {
    let op = if pressed { "key_down" } else { "key_up" };
    let seq = state.ref_seq.fetch_add(1, Ordering::Relaxed);
    // Phoenix channel wire format v2: [join_ref, ref, topic, event, payload]
    let msg = format!(
        r#"["1","{seq}","keyboard:tauri","key_event",{{"op":"{op}","key":"{key}"}}]"#
    );
    let _ = state.tx.send(msg);
}

// ── WebSocket task: connects to Phoenix socket and forwards key events ────

async fn run_keyboard_ws(mut rx: mpsc::UnboundedReceiver<String>) {
    let url = "ws://localhost:4000/socket/websocket?vsn=2.0.0";

    let Ok((ws, _)) = connect_async(url).await else {
        eprintln!("[nomos-tauri] keyboard ws: could not connect to {url}");
        return;
    };

    let (mut write, mut read) = ws.split();

    // Join the keyboard:tauri channel.
    let join = r#"[null,"1","keyboard:tauri","phx_join",{}]"#;
    if write.send(Message::Text(join.into())).await.is_err() {
        return;
    }

    // Drain incoming frames (Phoenix heartbeat replies, phx_reply acks, etc.).
    tauri::async_runtime::spawn(async move {
        while let Some(Ok(_)) = read.next().await {}
    });

    // Send/heartbeat loop: races key events against a 30s heartbeat timer.
    // Phoenix drops connections that haven't received a client heartbeat within
    // the server-side timeout (default 60s); 30s gives us a 2× safety margin.
    let mut hb_seq: u64 = 0;
    let mut heartbeat = time::interval(Duration::from_secs(30));
    heartbeat.tick().await; // consume the immediate first tick

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(msg) => {
                        if write.send(Message::Text(msg.into())).await.is_err() {
                            eprintln!("[nomos-tauri] keyboard ws: send error — connection lost");
                            break;
                        }
                    }
                    None => break, // sender dropped; Tauri is shutting down
                }
            }
            _ = heartbeat.tick() => {
                hb_seq += 1;
                let hb = format!(r#"[null,"{hb_seq}","phoenix","heartbeat",{{}}]"#);
                if write.send(Message::Text(hb.into())).await.is_err() {
                    eprintln!("[nomos-tauri] keyboard ws: heartbeat send error");
                    break;
                }
            }
        }
    }
}

// ── Entry point ───────────────────────────────────────────────────────────

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let (tx, rx) = mpsc::unbounded_channel::<String>();

    tauri::Builder::default()
        .setup(|app| {
            app.manage(KeyboardWs {
                tx,
                // Sequence starts at 2; "1" is reserved for the phx_join ref.
                ref_seq: AtomicU64::new(2),
            });

            tauri::async_runtime::spawn(run_keyboard_ws(rx));

            let _window = tauri::WebviewWindowBuilder::new(
                app,
                "main",
                tauri::WebviewUrl::External(
                    "http://localhost:4000"
                        .parse()
                        .expect("invalid nomos_beam URL"),
                ),
            )
            .title("nomos-studio")
            .inner_size(1280.0, 900.0)
            .build()?;

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![key_event])
        .run(tauri::generate_context!())
        .expect("error while running nomos-studio");
}
