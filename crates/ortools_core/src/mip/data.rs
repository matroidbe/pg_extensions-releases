//! Data structures for MIP problem specification.

#[derive(Debug)]
pub struct VariableData {
    pub name: String,
    pub var_type: String,
    pub domain_min: i64,
    pub domain_max: i64,
}

#[derive(Debug)]
pub struct ConstraintData {
    pub expression: String,
}

#[derive(Debug)]
pub struct ProblemData {
    #[allow(dead_code)]
    pub id: i64,
    pub variables: Vec<VariableData>,
    pub constraints: Vec<ConstraintData>,
    pub objective_type: Option<String>,
    pub objective_expr: Option<String>,
}
