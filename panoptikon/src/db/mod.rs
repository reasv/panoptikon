pub(crate) mod bookmarks;
mod connection;
pub(crate) mod epochs;
pub(crate) mod extraction_errors;
pub(crate) mod extraction_log;
pub(crate) mod extraction_write;
pub(crate) mod file_scans;
pub(crate) mod files;
pub(crate) mod folders;
pub(crate) mod index_writer;
pub(crate) mod info;
pub(crate) mod items;
pub(crate) mod ledger;
pub(crate) mod maintenance_state;
pub(crate) mod migrations;
pub(crate) mod pinboards;
pub(crate) mod pql;
pub(crate) mod prefix;
pub(crate) mod scan_errors;
pub(crate) mod setup;
pub(crate) mod sql_functions;
pub(crate) mod storage;
pub(crate) mod system_config;
pub(crate) mod tags;
pub(crate) mod vector_quants;
// TEMPORARY VERIFICATION HARNESS (int8 remap review) — remove with the file.
#[cfg(test)]
pub(crate) mod vq_int8_verify_harness;

#[allow(unused_imports)] // For the future DB delete/rename/restore flow.
pub(crate) use connection::invalidate_read_pools;
#[cfg(test)]
pub(crate) use connection::{open_index_db_read_at_path, readonly_test_override};
pub(crate) use connection::{
    DbConnection, ReadOnly, ReadOnlyNoUserData, UserDataWrite, ensure_migrations_allowed,
    open_index_db_read, open_index_db_read_no_user_data, open_index_db_write_no_user_data,
    readonly_mode,
};
