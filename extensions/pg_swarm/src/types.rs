//! Core types and state machine for pg_swarm

/// Task status values matching the CHECK constraint in the database
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatus::Pending => "pending",
            TaskStatus::Running => "running",
            TaskStatus::Completed => "completed",
            TaskStatus::Failed => "failed",
            TaskStatus::Cancelled => "cancelled",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(TaskStatus::Pending),
            "running" => Some(TaskStatus::Running),
            "completed" => Some(TaskStatus::Completed),
            "failed" => Some(TaskStatus::Failed),
            "cancelled" => Some(TaskStatus::Cancelled),
            _ => None,
        }
    }

    /// Check if a transition from self to target is valid
    pub fn can_transition_to(&self, target: TaskStatus) -> bool {
        matches!(
            (self, target),
            // Normal flow
            (TaskStatus::Pending, TaskStatus::Running)
                | (TaskStatus::Running, TaskStatus::Completed)
                | (TaskStatus::Running, TaskStatus::Failed)
                // Retry: failed -> pending
                | (TaskStatus::Failed, TaskStatus::Pending)
                // Cancellation
                | (TaskStatus::Pending, TaskStatus::Cancelled)
                | (TaskStatus::Running, TaskStatus::Cancelled)
                // Reassignment: running -> pending (dead node recovery)
                | (TaskStatus::Running, TaskStatus::Pending)
        )
    }
}

/// Job status values
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Pending => "pending",
            JobStatus::Running => "running",
            JobStatus::Completed => "completed",
            JobStatus::Failed => "failed",
            JobStatus::Cancelled => "cancelled",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(JobStatus::Pending),
            "running" => Some(JobStatus::Running),
            "completed" => Some(JobStatus::Completed),
            "failed" => Some(JobStatus::Failed),
            "cancelled" => Some(JobStatus::Cancelled),
            _ => None,
        }
    }
}

/// Node status values
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Active,
    Draining,
    Dead,
}

impl NodeStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeStatus::Active => "active",
            NodeStatus::Draining => "draining",
            NodeStatus::Dead => "dead",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "active" => Some(NodeStatus::Active),
            "draining" => Some(NodeStatus::Draining),
            "dead" => Some(NodeStatus::Dead),
            _ => None,
        }
    }
}

/// Scheduling strategy for task distribution
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulingStrategy {
    /// Any worker grabs the next available task (SKIP LOCKED)
    Greedy,
    /// Tasks are pre-assigned to specific nodes in round-robin order
    RoundRobin,
}

impl SchedulingStrategy {
    pub fn as_str(&self) -> &'static str {
        match self {
            SchedulingStrategy::Greedy => "greedy",
            SchedulingStrategy::RoundRobin => "round_robin",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "greedy" => Some(SchedulingStrategy::Greedy),
            "round_robin" => Some(SchedulingStrategy::RoundRobin),
            _ => None,
        }
    }
}

/// Executor health status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    /// Executor is working normally
    Healthy,
    /// Executor has some failures but still accepting tasks
    Degraded,
    /// Executor is disabled due to too many consecutive failures
    Disabled,
}

impl HealthStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            HealthStatus::Healthy => "healthy",
            HealthStatus::Degraded => "degraded",
            HealthStatus::Disabled => "disabled",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "healthy" => Some(HealthStatus::Healthy),
            "degraded" => Some(HealthStatus::Degraded),
            "disabled" => Some(HealthStatus::Disabled),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_status_roundtrip() {
        let statuses = [
            TaskStatus::Pending,
            TaskStatus::Running,
            TaskStatus::Completed,
            TaskStatus::Failed,
            TaskStatus::Cancelled,
        ];
        for status in &statuses {
            let s = status.as_str();
            let parsed = TaskStatus::from_str(s).unwrap();
            assert_eq!(*status, parsed);
        }
    }

    #[test]
    fn test_task_status_from_str_invalid() {
        assert!(TaskStatus::from_str("invalid").is_none());
        assert!(TaskStatus::from_str("").is_none());
    }

    #[test]
    fn test_task_valid_transitions() {
        // Normal flow
        assert!(TaskStatus::Pending.can_transition_to(TaskStatus::Running));
        assert!(TaskStatus::Running.can_transition_to(TaskStatus::Completed));
        assert!(TaskStatus::Running.can_transition_to(TaskStatus::Failed));

        // Retry
        assert!(TaskStatus::Failed.can_transition_to(TaskStatus::Pending));

        // Cancellation
        assert!(TaskStatus::Pending.can_transition_to(TaskStatus::Cancelled));
        assert!(TaskStatus::Running.can_transition_to(TaskStatus::Cancelled));

        // Dead node recovery
        assert!(TaskStatus::Running.can_transition_to(TaskStatus::Pending));
    }

    #[test]
    fn test_task_invalid_transitions() {
        // Cannot go backwards from completed
        assert!(!TaskStatus::Completed.can_transition_to(TaskStatus::Running));
        assert!(!TaskStatus::Completed.can_transition_to(TaskStatus::Pending));

        // Cannot go from cancelled
        assert!(!TaskStatus::Cancelled.can_transition_to(TaskStatus::Pending));
        assert!(!TaskStatus::Cancelled.can_transition_to(TaskStatus::Running));

        // Cannot skip running
        assert!(!TaskStatus::Pending.can_transition_to(TaskStatus::Completed));
        assert!(!TaskStatus::Pending.can_transition_to(TaskStatus::Failed));
    }

    #[test]
    fn test_job_status_roundtrip() {
        let statuses = [
            JobStatus::Pending,
            JobStatus::Running,
            JobStatus::Completed,
            JobStatus::Failed,
            JobStatus::Cancelled,
        ];
        for status in &statuses {
            let s = status.as_str();
            let parsed = JobStatus::from_str(s).unwrap();
            assert_eq!(*status, parsed);
        }
    }

    #[test]
    fn test_node_status_roundtrip() {
        let statuses = [NodeStatus::Active, NodeStatus::Draining, NodeStatus::Dead];
        for status in &statuses {
            let s = status.as_str();
            let parsed = NodeStatus::from_str(s).unwrap();
            assert_eq!(*status, parsed);
        }
    }

    #[test]
    fn test_scheduling_strategy_roundtrip() {
        let strategies = [SchedulingStrategy::Greedy, SchedulingStrategy::RoundRobin];
        for strategy in &strategies {
            let s = strategy.as_str();
            let parsed = SchedulingStrategy::from_str(s).unwrap();
            assert_eq!(*strategy, parsed);
        }
    }

    #[test]
    fn test_scheduling_strategy_from_str_invalid() {
        assert!(SchedulingStrategy::from_str("invalid").is_none());
        assert!(SchedulingStrategy::from_str("").is_none());
    }

    #[test]
    fn test_health_status_roundtrip() {
        let statuses = [
            HealthStatus::Healthy,
            HealthStatus::Degraded,
            HealthStatus::Disabled,
        ];
        for status in &statuses {
            let s = status.as_str();
            let parsed = HealthStatus::from_str(s).unwrap();
            assert_eq!(*status, parsed);
        }
    }

    #[test]
    fn test_health_status_from_str_invalid() {
        assert!(HealthStatus::from_str("broken").is_none());
    }
}
