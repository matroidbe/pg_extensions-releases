use std::ffi::{c_char, c_void};

/// Magic number from PG 18 oauth.h — identifies the validator ABI version
pub const PG_OAUTH_VALIDATOR_MAGIC: u32 = 0x20250220;

/// Corresponds to `ValidatorModuleState` in `src/include/libpq/oauth.h`
#[repr(C)]
pub struct ValidatorModuleState {
    pub sversion: i32,
    pub private_data: *mut c_void,
}

/// Corresponds to `ValidatorModuleResult` in `src/include/libpq/oauth.h`
#[repr(C)]
pub struct ValidatorModuleResult {
    pub authorized: bool,
    pub authn_id: *mut c_char,
}

/// Function pointer types matching the C typedefs in oauth.h
pub type ValidatorStartupCB = Option<unsafe extern "C-unwind" fn(state: *mut ValidatorModuleState)>;
pub type ValidatorShutdownCB =
    Option<unsafe extern "C-unwind" fn(state: *mut ValidatorModuleState)>;
pub type ValidatorValidateCB = Option<
    unsafe extern "C-unwind" fn(
        state: *const ValidatorModuleState,
        token: *const c_char,
        role: *const c_char,
        result: *mut ValidatorModuleResult,
    ) -> bool,
>;

/// Corresponds to `OAuthValidatorCallbacks` in `src/include/libpq/oauth.h`
#[repr(C)]
pub struct OAuthValidatorCallbacks {
    pub magic: u32,
    pub startup_cb: ValidatorStartupCB,
    pub shutdown_cb: ValidatorShutdownCB,
    pub validate_cb: ValidatorValidateCB,
}

// Safety: The callbacks struct is a static constant with function pointers only
unsafe impl Sync for OAuthValidatorCallbacks {}

/// Static callbacks struct — lives for the entire process lifetime.
/// PG 18 calls `_PG_oauth_validator_module_init()` to get a pointer to this.
static CALLBACKS: OAuthValidatorCallbacks = OAuthValidatorCallbacks {
    magic: PG_OAUTH_VALIDATOR_MAGIC,
    startup_cb: Some(crate::validator::startup_cb),
    shutdown_cb: Some(crate::validator::shutdown_cb),
    validate_cb: Some(crate::validator::validate_cb),
};

/// PG 18 entry point for OAuth validator module discovery.
/// Called when `oauth_validator_libraries` references this module.
/// On PG < 18, this symbol exists but is never called.
#[no_mangle]
pub extern "C-unwind" fn _PG_oauth_validator_module_init() -> *const OAuthValidatorCallbacks {
    &CALLBACKS
}
