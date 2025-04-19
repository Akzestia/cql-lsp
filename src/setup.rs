use dirs::data_dir;
use std::{fs::File, io::Write, path::PathBuf};

#[derive(Debug, Clone)]
pub struct SetupConfig {
    // Db related
    pub password: String,
    pub user_name: String,
    pub ip_addr: String,

    // Formatting & Suggestions
    pub context_based_select: bool,
}

pub fn setup_logger() -> Result<(), fern::InitError> {
    let mut log_path = data_dir().unwrap_or_else(|| PathBuf::from("."));
    log_path.push("cql_lsp");
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

pub fn setup_config() -> Result<(), Box<dyn std::error::Error>> {
    let mut config_path = data_dir().unwrap_or_else(|| PathBuf::from("."));
    config_path.push("cql_lsp/config.lsp");
    println!("Config: {:?}", config_path.to_str());

    if !config_path.exists() {
        let mut file =
            File::create_new(config_path).map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        write!(
            file,
            "[db_context]\npassword = \"{}\"\nuser = \"{}\"\nip = \"{}\"\n",
            "cassandara", "cassandara", "127.0.0.1"
        )?;

        return Ok(());
    }

    Ok(())
}
