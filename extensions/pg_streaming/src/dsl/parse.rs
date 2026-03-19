//! JSON parsing for pipeline definitions

use crate::dsl::types::PipelineDefinition;

/// Parse a JSON string into a PipelineDefinition
#[allow(dead_code)]
pub fn parse_pipeline(json: &str) -> Result<PipelineDefinition, String> {
    serde_json::from_str(json).map_err(|e| format!("Invalid pipeline definition: {}", e))
}

/// Parse a serde_json::Value into a PipelineDefinition
pub fn parse_pipeline_value(value: &serde_json::Value) -> Result<PipelineDefinition, String> {
    serde_json::from_value(value.clone()).map_err(|e| format!("Invalid pipeline definition: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid() {
        let json = r#"{
            "input": {"kafka": {"topic": "t"}},
            "pipeline": {"processors": []},
            "output": {"kafka": {"topic": "o"}}
        }"#;
        assert!(parse_pipeline(json).is_ok());
    }

    #[test]
    fn test_parse_missing_input() {
        let json = r#"{
            "pipeline": {"processors": []},
            "output": {"kafka": {"topic": "o"}}
        }"#;
        let result = parse_pipeline(json);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid pipeline definition"));
    }

    #[test]
    fn test_parse_missing_output() {
        let json = r#"{
            "input": {"kafka": {"topic": "t"}},
            "pipeline": {"processors": []}
        }"#;
        let result = parse_pipeline(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_unknown_connector() {
        let json = r#"{
            "input": {"unknown_connector": {"foo": "bar"}},
            "pipeline": {"processors": []},
            "output": {"kafka": {"topic": "o"}}
        }"#;
        let result = parse_pipeline(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_unknown_processor() {
        let json = r#"{
            "input": {"kafka": {"topic": "t"}},
            "pipeline": {"processors": [{"unknown_proc": {}}]},
            "output": {"kafka": {"topic": "o"}}
        }"#;
        let result = parse_pipeline(json);
        assert!(result.is_err());
    }
}
