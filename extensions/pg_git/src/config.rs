use pgrx::GucSetting;
use std::path::{Path, PathBuf};

pub static PG_GIT_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);
/// Default Git smart-HTTP listener port
pub const DEFAULT_HTTP_PORT: i32 = 5433;

pub static PG_GIT_HTTP_PORT: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_HTTP_PORT);
pub static PG_GIT_SYNC_INTERVAL: GucSetting<i32> = GucSetting::<i32>::new(5);
pub static PG_GIT_DEFAULT_REPO_PATH: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);
pub static PG_GIT_DATABASE: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);

pub fn register_gucs() {
    pgrx::GucRegistry::define_bool_guc(
        c"pg_git.enabled",
        c"Enable pg_git background workers",
        c"When true, the HTTP git server and sync workers start on server startup.",
        &PG_GIT_ENABLED,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_git.http_port",
        c"HTTP git endpoint port",
        c"The TCP port on which the HTTP git server listens for clone/push/fetch.",
        &PG_GIT_HTTP_PORT,
        1,
        65535,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_git.sync_interval",
        c"Sync interval in seconds",
        c"Seconds between periodic syncs of git repos to Postgres tables.",
        &PG_GIT_SYNC_INTERVAL,
        1,
        3600,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_git.default_repo_path",
        c"Default base path for git repositories",
        c"Base directory under which new repos are created when path is not specified. Empty means <data directory>/pg_git.",
        &PG_GIT_DEFAULT_REPO_PATH,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_git.database",
        c"Database for pg_git workers to connect to",
        c"The database where the pg_git extension is installed. Defaults to 'postgres'.",
        &PG_GIT_DATABASE,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );
}

/// Base directory for repos created without an explicit path: the
/// pg_git.default_repo_path GUC, else `<data directory>/pg_git`, which the
/// server can always create on any platform (design/pg_git, Configuration).
pub fn get_default_repo_path() -> PathBuf {
    let guc = pg_bgworker::guc_str(&PG_GIT_DEFAULT_REPO_PATH);
    resolve_repo_base(guc.as_deref(), data_directory().as_deref())
}

/// Pure resolution of the repo base: a non-blank GUC wins, else
/// `<data_dir>/pg_git`; outside a server, `pg_git` relative to the cwd.
pub fn resolve_repo_base(guc: Option<&str>, data_dir: Option<&Path>) -> PathBuf {
    match guc.map(str::trim).filter(|s| !s.is_empty()) {
        Some(path) => PathBuf::from(path),
        None => data_dir.unwrap_or(Path::new("")).join("pg_git"),
    }
}

/// The server's data directory, when running inside one.
fn data_directory() -> Option<PathBuf> {
    // SAFETY: DataDir is set once at postmaster start and never freed; a null
    // pointer (outside a server) is checked before it is read.
    unsafe {
        let dir = pgrx::pg_sys::DataDir;
        if dir.is_null() {
            return None;
        }
        std::ffi::CStr::from_ptr(dir)
            .to_str()
            .ok()
            .map(PathBuf::from)
    }
}

pub fn get_database() -> String {
    pg_bgworker::resolve_database(pg_bgworker::guc_str(&PG_GIT_DATABASE).as_deref(), None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn guc_path_wins() {
        assert_eq!(
            resolve_repo_base(Some("/srv/git"), Some(Path::new("/data"))),
            PathBuf::from("/srv/git")
        );
    }

    #[test]
    fn unset_guc_falls_back_to_data_directory() {
        assert_eq!(
            resolve_repo_base(None, Some(Path::new("/opt/homebrew/var/postgresql@18"))),
            PathBuf::from("/opt/homebrew/var/postgresql@18/pg_git")
        );
    }

    #[test]
    fn empty_guc_is_unset() {
        assert_eq!(
            resolve_repo_base(Some("  "), Some(Path::new("/data"))),
            PathBuf::from("/data/pg_git")
        );
    }

    #[test]
    fn outside_a_server_uses_relative_pg_git() {
        assert_eq!(resolve_repo_base(None, None), PathBuf::from("pg_git"));
    }
}
