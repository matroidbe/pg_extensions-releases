//! Processor chain — runs records through a sequence of processors

use crate::processor::Processor;
use crate::record::RecordBatch;

pub struct ProcessorChain {
    processors: Vec<Box<dyn Processor>>,
}

impl ProcessorChain {
    pub fn new(processors: Vec<Box<dyn Processor>>) -> Self {
        Self { processors }
    }

    /// Process a batch through all processors in sequence.
    /// If any processor returns an empty batch, short-circuit.
    pub fn process(&self, mut batch: RecordBatch) -> Result<RecordBatch, String> {
        for proc in &self.processors {
            if batch.is_empty() {
                return Ok(batch);
            }
            batch = proc
                .process(batch)
                .map_err(|e| format!("{}: {}", proc.name(), e))?;
        }
        Ok(batch)
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.processors.is_empty()
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.processors.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A test processor that always passes records through
    struct PassthroughProcessor;
    impl Processor for PassthroughProcessor {
        fn process(&self, batch: RecordBatch) -> Result<RecordBatch, String> {
            Ok(batch)
        }
        fn name(&self) -> &str {
            "passthrough"
        }
    }

    /// A test processor that drops all records
    struct DropAllProcessor;
    impl Processor for DropAllProcessor {
        fn process(&self, _batch: RecordBatch) -> Result<RecordBatch, String> {
            Ok(vec![])
        }
        fn name(&self) -> &str {
            "drop_all"
        }
    }

    /// A test processor that always errors
    struct ErrorProcessor;
    impl Processor for ErrorProcessor {
        fn process(&self, _batch: RecordBatch) -> Result<RecordBatch, String> {
            Err("intentional error".to_string())
        }
        fn name(&self) -> &str {
            "error"
        }
    }

    #[test]
    fn test_empty_chain() {
        let chain = ProcessorChain::new(vec![]);
        assert!(chain.is_empty());
        let batch = vec![serde_json::json!({"x": 1})];
        let result = chain.process(batch).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_passthrough_chain() {
        let chain = ProcessorChain::new(vec![
            Box::new(PassthroughProcessor),
            Box::new(PassthroughProcessor),
        ]);
        let batch = vec![serde_json::json!({"x": 1}), serde_json::json!({"x": 2})];
        let result = chain.process(batch).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_short_circuit_on_empty() {
        let chain = ProcessorChain::new(vec![
            Box::new(DropAllProcessor),
            Box::new(ErrorProcessor), // Should never be reached
        ]);
        let batch = vec![serde_json::json!({"x": 1})];
        let result = chain.process(batch).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_error_propagation() {
        let chain = ProcessorChain::new(vec![Box::new(ErrorProcessor)]);
        let batch = vec![serde_json::json!({"x": 1})];
        let result = chain.process(batch);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("error: intentional error"));
    }

    #[test]
    fn test_empty_batch_input() {
        let chain = ProcessorChain::new(vec![Box::new(PassthroughProcessor)]);
        let result = chain.process(vec![]).unwrap();
        assert!(result.is_empty());
    }
}
