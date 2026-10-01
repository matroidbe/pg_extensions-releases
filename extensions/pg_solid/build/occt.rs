//! OpenCASCADE (OCCT) discovery for the pg_solid build script.
//!
//! Kept in its own module so the probe logic is unit-testable — build scripts
//! are not part of cargo's test graph, so `#[cfg(test)]` code inside `build.rs`
//! itself would never run. `tests/occt_probe.rs` pulls this file into the test
//! crate via `#[path]`.
//!
//! # Finding OCCT
//!
//! Probed in order, first hit wins:
//!
//! 1. `OCCT_INCLUDE_DIR` (+ optional `OCCT_LIB_DIR`) — explicit override.
//! 2. `OCCT_ROOT` / `CASROOT` — a prefix containing `include/opencascade` and `lib`.
//! 3. `brew --prefix opencascade` — Homebrew on macOS / Linuxbrew.
//! 4. Well-known prefixes: `/usr`, `/usr/local`, `/opt/homebrew`, `/opt/local`.
//!
//! A package manager installing OCCT into a non-standard prefix should export
//! `OCCT_ROOT` (or the two `OCCT_*_DIR` vars) so this picks it up rather than
//! guessing.

#![allow(dead_code)] // Each consumer (build.rs, test crate) uses a subset.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Environment variables that can point the build at an OCCT installation.
pub const ENV_VARS: [&str; 4] = ["OCCT_INCLUDE_DIR", "OCCT_LIB_DIR", "OCCT_ROOT", "CASROOT"];

/// Subdirectories of a prefix that may hold OCCT headers.
const INCLUDE_SUBDIRS: [&str; 4] = ["include/opencascade", "inc", "include/oce", "include"];

/// Prefixes searched when nothing more specific is configured.
const WELL_KNOWN_PREFIXES: [&str; 4] = ["/usr", "/usr/local", "/opt/homebrew", "/opt/local"];

/// A located OCCT installation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Occt {
    /// Directory holding the OCCT headers (the one containing `Standard_Version.hxx`).
    pub include_dir: PathBuf,
    /// Directory holding `libTK*`, if one could be identified.
    pub lib_dir: Option<PathBuf>,
    /// `(major, minor)` parsed from `Standard_Version.hxx`.
    pub version: Option<(u32, u32)>,
}

impl Occt {
    /// Locate an OCCT installation, or return `None`.
    pub fn probe(target_arch: Option<&str>) -> Option<Self> {
        // 1. An explicit include dir wins outright.
        if let Some(include_dir) = env_dir("OCCT_INCLUDE_DIR") {
            if has_headers(&include_dir) {
                let version = parse_version(&include_dir);
                return Some(Occt {
                    include_dir,
                    lib_dir: env_dir("OCCT_LIB_DIR"),
                    version,
                });
            }
        }

        // 2. Prefix-shaped overrides, then Homebrew, then well-known prefixes.
        let mut prefixes: Vec<PathBuf> = Vec::new();
        for var in ["OCCT_ROOT", "CASROOT"] {
            if let Some(dir) = env_dir(var) {
                prefixes.push(dir);
            }
        }
        if let Some(dir) = brew_prefix("opencascade") {
            prefixes.push(dir);
        }
        prefixes.extend(WELL_KNOWN_PREFIXES.iter().map(PathBuf::from));

        prefixes
            .iter()
            .find_map(|prefix| Self::from_prefix(prefix, target_arch))
    }

    /// Build an `Occt` from an installation prefix, if it looks like one.
    pub fn from_prefix(prefix: &Path, target_arch: Option<&str>) -> Option<Self> {
        // OCCT headers are conventionally installed flat inside an
        // `opencascade` subdirectory; some source builds drop them in `inc`.
        let include_dir = INCLUDE_SUBDIRS
            .iter()
            .map(|sub| prefix.join(sub))
            .find(|dir| has_headers(dir))?;

        let lib_dir = env_dir("OCCT_LIB_DIR").or_else(|| find_lib_dir(prefix, target_arch));
        let version = parse_version(&include_dir);

        Some(Occt {
            include_dir,
            lib_dir,
            version,
        })
    }

    /// Human-readable version, for build diagnostics.
    pub fn version_string(&self) -> String {
        self.version
            .map(|(major, minor)| format!("{major}.{minor}"))
            .unwrap_or_else(|| "unknown version".into())
    }
}

/// The OCCT toolkits pg_solid links against.
///
/// Everything except STEP/IGES is stable across releases. For the data-exchange
/// toolkits the names depend on the version: OCCT >= 7.8 merged the STEP
/// toolkits into `TKDESTEP` and renamed `TKIGES` to `TKDEIGES`.
///
/// The toolkit rename is not the only 7.8 difference — `STEPControl_Writer`
/// only gained `WriteStream` in 7.8, so `src/cpp/occt_wrapper.cpp` carries an
/// `OCC_VERSION_HEX` fallback that writes via a temp file on older releases.
/// Keep the two version gates in sync.
///
/// An unparseable version is treated as modern, which reproduces the behaviour
/// this build script had before version detection existed.
pub fn toolkits(version: Option<(u32, u32)>) -> Vec<&'static str> {
    let mut libs = vec![
        "TKernel",     // Foundation: handles, memory, math primitives
        "TKMath",      // 3D geometry primitives: gp_Pnt, gp_Vec
        "TKBRep",      // B-Rep data structures: TopoDS_Shape
        "TKGeomBase",  // Geometric curves and surfaces
        "TKG3d",       // 3D geometry
        "TKG2d",       // 2D geometry
        "TKTopAlgo",   // Topological algorithms, BRepExtrema
        "TKPrim",      // Primitive constructors: MakeBox, MakeCylinder
        "TKShHealing", // Shape healing: ShapeFix_Shape
        "TKGeomAlgo",  // Geometric algorithms (required by TKBO)
        "TKBO",        // Boolean operation kernel
        "TKBool",      // BRepAlgoAPI_Fuse/Cut/Common
        "TKMesh",      // BRepMesh_IncrementalMesh for STL tessellation
        "TKOffset",    // Offset/shell operations (BRepOffsetAPI_MakeOffsetShape)
        "TKHLR",       // Hidden Line Removal for 2D projection
    ];

    if uses_tkde_dataexchange(version) {
        // OCCT >= 7.8: DataExchange regrouped under the TKDE* prefix.
        libs.push("TKDESTEP"); // STEP import/export (STEPControl_Reader/Writer)
        libs.push("TKDEIGES"); // IGES import/export (IGESControl_Reader/Writer)
    } else {
        // OCCT < 7.8: per-protocol STEP toolkits, plus the shared data-exchange
        // base (TKXSBase) that the TKDE* libraries later subsumed.
        libs.push("TKXSBase"); // Data-exchange base (IFSelect_*, Interface_*)
        libs.push("TKSTEPBase");
        libs.push("TKSTEPAttr");
        libs.push("TKSTEP209");
        libs.push("TKSTEP"); // STEPControl_Reader/Writer
        libs.push("TKIGES"); // IGESControl_Reader/Writer
    }

    libs
}

/// Does this OCCT version use the 7.8+ `TKDE*` DataExchange toolkit names?
pub fn uses_tkde_dataexchange(version: Option<(u32, u32)>) -> bool {
    match version {
        Some(v) => v >= (7, 8),
        None => true,
    }
}

/// Does this directory hold OCCT headers?
pub fn has_headers(dir: &Path) -> bool {
    dir.join("Standard_Version.hxx").is_file()
}

/// Read `OCC_VERSION_MAJOR` / `OCC_VERSION_MINOR` from `Standard_Version.hxx`.
pub fn parse_version(include_dir: &Path) -> Option<(u32, u32)> {
    let text = std::fs::read_to_string(include_dir.join("Standard_Version.hxx")).ok()?;
    parse_version_header(&text)
}

/// Extract `(major, minor)` from the text of `Standard_Version.hxx`.
pub fn parse_version_header(text: &str) -> Option<(u32, u32)> {
    Some((
        parse_define(text, "OCC_VERSION_MAJOR")?,
        parse_define(text, "OCC_VERSION_MINOR")?,
    ))
}

/// Extract the integer value of `#define <name> <int>` from a header.
pub fn parse_define(text: &str, name: &str) -> Option<u32> {
    text.lines().find_map(|line| {
        let rest = line.trim().strip_prefix("#define")?;
        let rest = rest.trim_start().strip_prefix(name)?;
        // Guard against `OCC_VERSION_MAJOR` matching `OCC_VERSION_MAJOR_EXT`.
        if !rest.starts_with(char::is_whitespace) {
            return None;
        }
        rest.split_whitespace().next()?.parse().ok()
    })
}

/// Candidate library directories under a prefix, most specific first.
pub fn lib_dir_candidates(prefix: &Path, target_arch: Option<&str>) -> Vec<PathBuf> {
    let mut candidates = vec![prefix.join("lib"), prefix.join("lib64")];
    // Debian/Ubuntu multiarch, e.g. /usr/lib/x86_64-linux-gnu
    if let Some(arch) = target_arch {
        candidates.push(prefix.join("lib").join(format!("{arch}-linux-gnu")));
    }
    candidates
}

/// Find the directory under `prefix` that actually contains `libTKernel`.
pub fn find_lib_dir(prefix: &Path, target_arch: Option<&str>) -> Option<PathBuf> {
    lib_dir_candidates(prefix, target_arch)
        .into_iter()
        .find(|dir| {
            ["libTKernel.so", "libTKernel.dylib", "libTKernel.a"]
                .iter()
                .any(|f| dir.join(f).exists())
        })
}

/// Is this a directory the dynamic loader already searches by default?
///
/// Anything else needs an rpath baked into the extension so the PostgreSQL
/// backend can resolve OCCT at load time without ldconfig or LD_LIBRARY_PATH.
pub fn is_system_lib_dir(dir: &Path) -> bool {
    let path = dir.to_string_lossy();
    matches!(path.as_ref(), "/lib" | "/lib64" | "/usr/lib" | "/usr/lib64")
        || path.starts_with("/usr/lib/")
        || path.starts_with("/lib/")
}

/// An existing directory named by an environment variable.
pub fn env_dir(var: &str) -> Option<PathBuf> {
    let value = std::env::var(var).ok()?;
    let trimmed = value.trim();
    let path = PathBuf::from(trimmed);
    (!trimmed.is_empty() && path.is_dir()).then_some(path)
}

/// `brew --prefix <formula>`, if Homebrew is installed and has the formula.
pub fn brew_prefix(formula: &str) -> Option<PathBuf> {
    let output = Command::new("brew")
        .arg("--prefix")
        .arg(formula)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let path = PathBuf::from(String::from_utf8(output.stdout).ok()?.trim());
    path.is_dir().then_some(path)
}

/// Actionable error when OCCT cannot be located.
pub fn not_found_message() -> String {
    let install_hint = if cfg!(target_os = "macos") {
        "  brew install opencascade"
    } else if Path::new("/etc/debian_version").exists() {
        "  sudo apt install libocct-foundation-dev libocct-modeling-data-dev \\\n\
         \x20      libocct-modeling-algorithms-dev libocct-data-exchange-dev"
    } else if Path::new("/etc/redhat-release").exists() {
        "  sudo dnf install opencascade-devel"
    } else if Path::new("/etc/arch-release").exists() {
        "  sudo pacman -S opencascade"
    } else {
        "  Install the OpenCASCADE development package for your platform."
    };

    format!(
        "\n\npg_solid: could not find an OpenCASCADE (OCCT) installation.\n\n\
         Install it:\n{install_hint}\n\n\
         Or point the build at an existing installation:\n\
         \x20 OCCT_ROOT=/path/to/prefix          (expects <prefix>/include/opencascade)\n\
         \x20 OCCT_INCLUDE_DIR=/path/to/headers  (the directory holding Standard_Version.hxx)\n\
         \x20 OCCT_LIB_DIR=/path/to/libs         (the directory holding libTK*)\n\n\
         OCCT 7.6 and newer are supported.\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shaped like the real header: `#define` padded with multiple spaces, and
    /// preceded by comment lines that mention the same macro names.
    const HEADER_7_6: &str = "\
// OCC_VERSION_MAJOR       : (integer) number identifying major version
#define OCC_VERSION_MAJOR         7
#define OCC_VERSION_MINOR         6
#define OCC_VERSION_MAINTENANCE   3
";

    #[test]
    fn parses_version_defines() {
        assert_eq!(parse_define(HEADER_7_6, "OCC_VERSION_MAJOR"), Some(7));
        assert_eq!(parse_define(HEADER_7_6, "OCC_VERSION_MINOR"), Some(6));
        assert_eq!(parse_define(HEADER_7_6, "OCC_VERSION_MAINTENANCE"), Some(3));
    }

    #[test]
    fn parses_version_pair() {
        assert_eq!(parse_version_header(HEADER_7_6), Some((7, 6)));
    }

    #[test]
    fn comment_lines_are_not_mistaken_for_defines() {
        // The real header documents the macros in `//` comments above them.
        let comments_only = "// OCC_VERSION_MAJOR : (integer) number identifying major version\n";
        assert_eq!(parse_define(comments_only, "OCC_VERSION_MAJOR"), None);
    }

    #[test]
    fn define_does_not_match_longer_name() {
        let header = "#define OCC_VERSION_MAJOR_EXT 9\n#define OCC_VERSION_MAJOR 7\n";
        assert_eq!(parse_define(header, "OCC_VERSION_MAJOR"), Some(7));
    }

    #[test]
    fn missing_define_is_none() {
        assert_eq!(parse_define("#define OTHER 1\n", "OCC_VERSION_MAJOR"), None);
        assert_eq!(parse_version_header("#define OCC_VERSION_MAJOR 7\n"), None);
    }

    #[test]
    fn pre_78_uses_legacy_step_toolkits() {
        for version in [(7, 5), (7, 6), (7, 7)] {
            let libs = toolkits(Some(version));
            assert!(libs.contains(&"TKSTEP"), "{version:?} should link TKSTEP");
            assert!(libs.contains(&"TKIGES"), "{version:?} should link TKIGES");
            assert!(
                libs.contains(&"TKXSBase"),
                "{version:?} needs the data-exchange base"
            );
            assert!(
                !libs.contains(&"TKDESTEP"),
                "TKDESTEP does not exist before 7.8"
            );
            assert!(
                !libs.contains(&"TKDEIGES"),
                "TKDEIGES does not exist before 7.8"
            );
        }
    }

    #[test]
    fn v78_and_later_use_tkde_toolkits() {
        for version in [(7, 8), (7, 9), (8, 0)] {
            let libs = toolkits(Some(version));
            assert!(
                libs.contains(&"TKDESTEP"),
                "{version:?} should link TKDESTEP"
            );
            assert!(
                libs.contains(&"TKDEIGES"),
                "{version:?} should link TKDEIGES"
            );
            assert!(
                !libs.contains(&"TKSTEP"),
                "{version:?} must not link the removed TKSTEP"
            );
        }
    }

    #[test]
    fn unknown_version_assumes_modern() {
        assert!(uses_tkde_dataexchange(None));
        assert!(toolkits(None).contains(&"TKDESTEP"));
    }

    #[test]
    fn core_toolkits_are_version_independent() {
        let legacy = toolkits(Some((7, 6)));
        let modern = toolkits(Some((7, 8)));
        for lib in ["TKernel", "TKMath", "TKBRep", "TKBO", "TKMesh", "TKHLR"] {
            assert!(legacy.contains(&lib), "{lib} missing from 7.6 link line");
            assert!(modern.contains(&lib), "{lib} missing from 7.8 link line");
        }
    }

    #[test]
    fn no_duplicate_toolkits() {
        for version in [Some((7, 6)), Some((7, 8)), None] {
            let libs = toolkits(version);
            let mut sorted = libs.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(
                sorted.len(),
                libs.len(),
                "duplicate -l flag for {version:?}"
            );
        }
    }

    #[test]
    fn system_lib_dirs_need_no_rpath() {
        for dir in [
            "/usr/lib",
            "/usr/lib64",
            "/lib",
            "/usr/lib/x86_64-linux-gnu",
        ] {
            assert!(is_system_lib_dir(Path::new(dir)), "{dir} is a system dir");
        }
    }

    #[test]
    fn non_system_lib_dirs_need_an_rpath() {
        for dir in [
            "/opt/homebrew/lib",
            "/usr/local/lib",
            "/home/me/.pgbrew/vendor/occt/lib",
        ] {
            assert!(!is_system_lib_dir(Path::new(dir)), "{dir} needs an rpath");
        }
    }

    #[test]
    fn multiarch_candidate_is_offered_when_arch_known() {
        let candidates = lib_dir_candidates(Path::new("/usr"), Some("x86_64"));
        assert!(candidates.contains(&PathBuf::from("/usr/lib/x86_64-linux-gnu")));
        // ...and omitted when it isn't.
        let bare = lib_dir_candidates(Path::new("/usr"), None);
        assert!(!bare
            .iter()
            .any(|p| p.to_string_lossy().contains("linux-gnu")));
    }

    #[test]
    fn missing_prefix_yields_nothing() {
        let missing = Path::new("/nonexistent-prefix-for-pg-solid-tests");
        assert!(!has_headers(missing));
        assert!(find_lib_dir(missing, Some("x86_64")).is_none());
        assert!(Occt::from_prefix(missing, Some("x86_64")).is_none());
    }

    #[test]
    fn env_dir_rejects_empty_and_missing() {
        // A var that is certainly not set.
        assert!(env_dir("PG_SOLID_DEFINITELY_UNSET_VAR").is_none());
    }

    #[test]
    fn not_found_message_is_actionable() {
        let msg = not_found_message();
        assert!(msg.contains("OCCT_ROOT"));
        assert!(msg.contains("OCCT_INCLUDE_DIR"));
        assert!(msg.contains("OCCT_LIB_DIR"));
    }

    #[test]
    fn version_string_handles_unknown() {
        let occt = Occt {
            include_dir: PathBuf::from("/tmp"),
            lib_dir: None,
            version: None,
        };
        assert_eq!(occt.version_string(), "unknown version");
        assert_eq!(
            Occt {
                version: Some((7, 6)),
                ..occt
            }
            .version_string(),
            "7.6"
        );
    }
}
