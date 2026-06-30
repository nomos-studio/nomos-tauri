#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .setup(|app| {
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
        .run(tauri::generate_context!())
        .expect("error while running nomos-studio");
}
