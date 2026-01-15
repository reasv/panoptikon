use anyhow::{Context, Result};
use std::{env, fs, path::PathBuf};

use crate::policy::{DbInfo, SingleDbInfo};

pub(crate) fn load_db_info() -> Result<DbInfo> {
    let (index_default, user_default) = db_defaults();
    let (index_dbs, user_data_dbs) = db_lists()?;
    Ok(DbInfo {
        index: SingleDbInfo {
            current: index_default,
            all: index_dbs,
        },
        user_data: SingleDbInfo {
            current: user_default,
            all: user_data_dbs,
        },
    })
}

fn db_defaults() -> (String, String) {
    let index_default = env::var("INDEX_DB").unwrap_or_else(|_| "default".to_string());
    let user_default = env::var("USER_DATA_DB").unwrap_or_else(|_| "default".to_string());
    (index_default, user_default)
}

fn db_lists() -> Result<(Vec<String>, Vec<String>)> {
    let data_dir = PathBuf::from(env::var("DATA_FOLDER").unwrap_or_else(|_| "data".to_string()));
    let index_dir = data_dir.join("index");
    let user_data_dir = data_dir.join("user_data");

    fs::create_dir_all(&index_dir)
        .with_context(|| format!("failed to create index dir {}", index_dir.display()))?;
    fs::create_dir_all(&user_data_dir)
        .with_context(|| format!("failed to create user data dir {}", user_data_dir.display()))?;

    let mut index_dbs = Vec::new();
    for entry in fs::read_dir(&index_dir)
        .with_context(|| format!("failed to read index dir {}", index_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() && path.join("index.db").exists() {
            if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
                index_dbs.push(name.to_string());
            }
        }
    }

    let mut user_data_dbs = Vec::new();
    for entry in fs::read_dir(&user_data_dir)
        .with_context(|| format!("failed to read user data dir {}", user_data_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("db") {
            if let Some(stem) = path.file_stem().and_then(|name| name.to_str()) {
                user_data_dbs.push(stem.to_string());
            }
        }
    }

    Ok((index_dbs, user_data_dbs))
}
