//! Queryable fitted state: expose scaler means/stds, encoder categories,
//! imputer fill values, etc. from a trained model's pipeline.

use crate::error::AugurPgError;
use crate::models;
use pgrx::prelude::*;

fn err<T>(res: Result<T, AugurPgError>) -> T {
    res.unwrap_or_else(|e| pgrx::error!("pg_augur: {e}"))
}

/// Returns the fitted preprocessing parameters for a trained project/feature view.
///
/// Parses the deployed model's preprocessing JSON and flattens each
/// transformer step into (column_name, transformer, param_name, param_value) rows.
#[pg_extern]
fn fitted_params(
    project_name: &str,
) -> TableIterator<
    'static,
    (
        name!(column_name, String),
        name!(transformer, String),
        name!(param_name, String),
        name!(param_value, String),
    ),
> {
    let stored = err(models::get_deployed_model(project_name));
    let json_str = std::str::from_utf8(&stored.artifact)
        .map_err(|e| AugurPgError::Other(format!("artifact not valid utf-8: {e}")))
        .unwrap_or_else(|e| pgrx::error!("pg_augur: {e}"));
    let spec = err(augur::prelude::load_model_from_string(json_str).map_err(AugurPgError::from));

    let rows = err(extract_pipeline_params(&spec.preprocessing));
    TableIterator::new(rows)
}

/// Extract per-column parameters from a pipeline state JSON array.
fn extract_pipeline_params(
    preprocessing: &serde_json::Value,
) -> Result<Vec<(String, String, String, String)>, AugurPgError> {
    let mut rows = Vec::new();

    let steps = preprocessing
        .as_array()
        .ok_or_else(|| AugurPgError::Json("preprocessing state is not an array".into()))?;

    for step in steps {
        let transformer_type = step
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let state = match step.get("state") {
            Some(s) => s,
            None => continue,
        };

        match transformer_type {
            // ── Imputers ──
            "MeanImputer" => {
                if let Some(means) = state.get("means").and_then(|v| v.as_object()) {
                    for (col, val) in means {
                        rows.push((
                            col.clone(),
                            "MeanImputer".to_string(),
                            "fill_value".to_string(),
                            format_json_number(val),
                        ));
                    }
                }
            }
            "MedianImputer" => {
                if let Some(medians) = state.get("medians").and_then(|v| v.as_object()) {
                    for (col, val) in medians {
                        rows.push((
                            col.clone(),
                            "MedianImputer".to_string(),
                            "fill_value".to_string(),
                            format_json_number(val),
                        ));
                    }
                }
            }
            "ModeImputer" => {
                if let Some(modes) = state.get("numeric_modes").and_then(|v| v.as_object()) {
                    for (col, val) in modes {
                        rows.push((
                            col.clone(),
                            "ModeImputer".to_string(),
                            "fill_value".to_string(),
                            format_json_number(val),
                        ));
                    }
                }
                if let Some(modes) = state.get("categorical_modes").and_then(|v| v.as_object()) {
                    for (col, val) in modes {
                        rows.push((
                            col.clone(),
                            "ModeImputer".to_string(),
                            "fill_value".to_string(),
                            val.as_str().unwrap_or("").to_string(),
                        ));
                    }
                }
            }

            // ── Scalers ──
            "StandardScaler" => {
                let means = state.get("means").and_then(|v| v.as_object());
                let stds = state.get("stds").and_then(|v| v.as_object());
                if let Some(means) = means {
                    for (col, val) in means {
                        rows.push((
                            col.clone(),
                            "StandardScaler".to_string(),
                            "mean".to_string(),
                            format_json_number(val),
                        ));
                    }
                }
                if let Some(stds) = stds {
                    for (col, val) in stds {
                        rows.push((
                            col.clone(),
                            "StandardScaler".to_string(),
                            "std".to_string(),
                            format_json_number(val),
                        ));
                    }
                }
            }
            "MinMaxScaler" => {
                if let Some(mins) = state.get("mins").and_then(|v| v.as_object()) {
                    for (col, val) in mins {
                        rows.push((
                            col.clone(),
                            "MinMaxScaler".to_string(),
                            "min".to_string(),
                            format_json_number(val),
                        ));
                    }
                }
                if let Some(ranges) = state.get("ranges").and_then(|v| v.as_object()) {
                    for (col, val) in ranges {
                        rows.push((
                            col.clone(),
                            "MinMaxScaler".to_string(),
                            "range".to_string(),
                            format_json_number(val),
                        ));
                    }
                }
            }
            "RobustScaler" => {
                if let Some(medians) = state.get("medians").and_then(|v| v.as_object()) {
                    for (col, val) in medians {
                        rows.push((
                            col.clone(),
                            "RobustScaler".to_string(),
                            "median".to_string(),
                            format_json_number(val),
                        ));
                    }
                }
                if let Some(iqrs) = state.get("iqrs").and_then(|v| v.as_object()) {
                    for (col, val) in iqrs {
                        rows.push((
                            col.clone(),
                            "RobustScaler".to_string(),
                            "iqr".to_string(),
                            format_json_number(val),
                        ));
                    }
                }
            }
            "MaxAbsScaler" => {
                if let Some(max_abs) = state.get("max_abs").and_then(|v| v.as_object()) {
                    for (col, val) in max_abs {
                        rows.push((
                            col.clone(),
                            "MaxAbsScaler".to_string(),
                            "max_abs".to_string(),
                            format_json_number(val),
                        ));
                    }
                }
            }

            // ── Encoders ──
            "OneHotEncoder" => {
                if let Some(cats) = state.get("categories").and_then(|v| v.as_object()) {
                    for (col, val) in cats {
                        rows.push((
                            col.clone(),
                            "OneHotEncoder".to_string(),
                            "categories".to_string(),
                            val.to_string(),
                        ));
                    }
                }
            }
            "LabelEncoder" | "OrdinalEncoder" | "TargetEncoder" => {
                if let Some(mappings) = state.get("mappings").and_then(|v| v.as_object()) {
                    for (col, val) in mappings {
                        rows.push((
                            col.clone(),
                            transformer_type.to_string(),
                            "mappings".to_string(),
                            val.to_string(),
                        ));
                    }
                }
                if let Some(gm) = state.get("global_mean") {
                    rows.push((
                        "(global)".to_string(),
                        transformer_type.to_string(),
                        "global_mean".to_string(),
                        format_json_number(gm),
                    ));
                }
            }

            // ── Feature selection ──
            "VarianceThresholdSelector" | "CorrelationFilter" => {
                if let Some(kept) = state.get("kept_columns").and_then(|v| v.as_array()) {
                    for col in kept {
                        if let Some(name) = col.as_str() {
                            rows.push((
                                name.to_string(),
                                transformer_type.to_string(),
                                "status".to_string(),
                                "kept".to_string(),
                            ));
                        }
                    }
                }
                if let Some(removed) = state.get("removed_columns").and_then(|v| v.as_array()) {
                    for col in removed {
                        if let Some(name) = col.as_str() {
                            rows.push((
                                name.to_string(),
                                transformer_type.to_string(),
                                "status".to_string(),
                                "removed".to_string(),
                            ));
                        }
                    }
                }
            }

            // ── PCA ──
            "PcaTransformer" => {
                if let Some(n) = state.get("n_components").and_then(|v| v.as_u64()) {
                    rows.push((
                        "(pca)".to_string(),
                        "PcaTransformer".to_string(),
                        "n_components".to_string(),
                        n.to_string(),
                    ));
                }
                if let Some(ratio) = state.get("explained_variance_ratio") {
                    rows.push((
                        "(pca)".to_string(),
                        "PcaTransformer".to_string(),
                        "explained_variance_ratio".to_string(),
                        ratio.to_string(),
                    ));
                }
            }

            // ── New transformers ──
            "MissingIndicator" => {
                if let Some(cols) = state.get("columns_with_nulls").and_then(|v| v.as_array()) {
                    for col in cols {
                        if let Some(name) = col.as_str() {
                            rows.push((
                                name.to_string(),
                                "MissingIndicator".to_string(),
                                "has_missing".to_string(),
                                "true".to_string(),
                            ));
                        }
                    }
                }
            }
            "Winsorizer" => {
                if let Some(bounds) = state.get("bounds").and_then(|v| v.as_object()) {
                    for (col, val) in bounds {
                        rows.push((
                            col.clone(),
                            "Winsorizer".to_string(),
                            "bounds".to_string(),
                            val.to_string(),
                        ));
                    }
                }
            }
            "DatetimeExtractor" => {
                if let Some(cols) = state.get("columns").and_then(|v| v.as_array()) {
                    for col in cols {
                        let name = col.get("name").and_then(|v| v.as_str()).unwrap_or("?");
                        let kind = col.get("kind").and_then(|v| v.as_str()).unwrap_or("?");
                        rows.push((
                            name.to_string(),
                            "DatetimeExtractor".to_string(),
                            "kind".to_string(),
                            kind.to_string(),
                        ));
                    }
                }
            }
            "FrequencyEncoder" => {
                if let Some(freqs) = state.get("frequencies").and_then(|v| v.as_object()) {
                    for (col, val) in freqs {
                        rows.push((
                            col.clone(),
                            "FrequencyEncoder".to_string(),
                            "frequencies".to_string(),
                            val.to_string(),
                        ));
                    }
                }
            }
            "KBinsDiscretizer" => {
                if let Some(edges) = state.get("bin_edges").and_then(|v| v.as_object()) {
                    for (col, val) in edges {
                        rows.push((
                            col.clone(),
                            "KBinsDiscretizer".to_string(),
                            "bin_edges".to_string(),
                            val.to_string(),
                        ));
                    }
                }
            }
            "InteractionTransformer" => {
                if let Some(cols) = state.get("columns").and_then(|v| v.as_array()) {
                    for col in cols {
                        if let Some(name) = col.as_str() {
                            rows.push((
                                name.to_string(),
                                "InteractionTransformer".to_string(),
                                "column".to_string(),
                                "included".to_string(),
                            ));
                        }
                    }
                }
            }
            "MutualInfoSelector" => {
                if let Some(scores) = state.get("scores").and_then(|v| v.as_object()) {
                    for (col, val) in scores {
                        rows.push((
                            col.clone(),
                            "MutualInfoSelector".to_string(),
                            "mi_score".to_string(),
                            format_json_number(val),
                        ));
                    }
                }
                if let Some(kept) = state.get("kept_columns").and_then(|v| v.as_array()) {
                    for col in kept {
                        if let Some(name) = col.as_str() {
                            rows.push((
                                name.to_string(),
                                "MutualInfoSelector".to_string(),
                                "status".to_string(),
                                "kept".to_string(),
                            ));
                        }
                    }
                }
            }

            _ => {
                // Unknown transformer — emit raw state
                rows.push((
                    "(all)".to_string(),
                    transformer_type.to_string(),
                    "state".to_string(),
                    state.to_string(),
                ));
            }
        }
    }

    Ok(rows)
}

fn format_json_number(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                format!("{}", f)
            } else {
                n.to_string()
            }
        }
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_standard_scaler_state() {
        let pipeline_json = serde_json::json!([
            {
                "name": "scaler_0",
                "type": "StandardScaler",
                "state": {
                    "means": {"age": 45.5, "income": 52000.0},
                    "stds": {"age": 15.2, "income": 18000.0}
                }
            }
        ]);
        let rows = extract_pipeline_params(&pipeline_json).unwrap();
        assert_eq!(rows.len(), 4); // 2 means + 2 stds
        assert!(rows
            .iter()
            .any(|(col, _, param, _)| col == "age" && param == "mean"));
        assert!(rows
            .iter()
            .any(|(col, _, param, _)| col == "age" && param == "std"));
    }

    #[test]
    fn extract_imputer_state() {
        let pipeline_json = serde_json::json!([
            {
                "name": "imputer_0",
                "type": "MeanImputer",
                "state": {
                    "means": {"age": 35.0, "salary": 60000.0}
                }
            }
        ]);
        let rows = extract_pipeline_params(&pipeline_json).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows
            .iter()
            .any(|(col, t, _, _)| col == "age" && t == "MeanImputer"));
    }

    #[test]
    fn extract_encoder_state() {
        let pipeline_json = serde_json::json!([
            {
                "name": "encoder_0",
                "type": "OneHotEncoder",
                "state": {
                    "categories": {
                        "color": ["blue", "green", "red"],
                        "size": ["L", "M", "S"]
                    }
                }
            }
        ]);
        let rows = extract_pipeline_params(&pipeline_json).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows
            .iter()
            .any(|(col, _, param, _)| col == "color" && param == "categories"));
    }

    #[test]
    fn extract_empty_pipeline() {
        let pipeline_json = serde_json::json!([]);
        let rows = extract_pipeline_params(&pipeline_json).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn extract_multi_step_pipeline() {
        let pipeline_json = serde_json::json!([
            {
                "name": "imputer_0",
                "type": "MeanImputer",
                "state": {"means": {"x": 5.0}}
            },
            {
                "name": "scaler_0",
                "type": "StandardScaler",
                "state": {"means": {"x": 5.0}, "stds": {"x": 2.0}}
            }
        ]);
        let rows = extract_pipeline_params(&pipeline_json).unwrap();
        assert_eq!(rows.len(), 3); // 1 imputer + 2 scaler
    }
}
