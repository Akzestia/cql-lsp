use dirs::data_dir;
use std::path::PathBuf;

pub fn setup_logger() -> Result<(), fern::InitError> {
    let mut log_path = data_dir().unwrap_or_else(|| PathBuf::from("."));
    log_path.push("cql-lsp");
    std::fs::create_dir_all(&log_path).expect("Failed to create log directory");
    log_path.push("output.log");

    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{} {} {}] {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                record.target(),
                message
            ))
        })
        .level(log::LevelFilter::Info)
        .chain(std::io::stdout())
        .chain(fern::log_file(log_path)?)
        .apply()?;

    Ok(())
}

pub fn setup_config() -> Result<(), std::fmt::Error> {
    Ok(())
}
