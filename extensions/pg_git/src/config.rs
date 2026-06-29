use pgrx::GucSetting;

pub static PG_GIT_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static PG_GIT_HTTP_PORT: GucSetting<i32> = GucSetting::<i32>::new(5433);
pub static PG_GIT_SYNC_INTERVAL: GucSetting<i32> = GucSetting::<i32>::new(5);
pub static PG_GIT_DEFAULT_REPO_PATH: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);
pub static PG_GIT_DATABASE: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);

const DEFAULT_REPO_PATH: &str = "/var/lib/pg_git";

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
        c"Base directory under which new repos are created when path is not specified.",
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

pub fn get_default_repo_path() -> String {
    PG_GIT_DEFAULT_REPO_PATH
        .get()
        .as_ref()
        .and_then(|s| s.to_str().ok())
        .unwrap_or(DEFAULT_REPO_PATH)
        .to_string()
}

pub fn get_database() -> String {
    PG_GIT_DATABASE
        .get()
        .as_ref()
        .and_then(|s| s.to_str().ok())
        .unwrap_or("postgres")
        .to_string()
}
