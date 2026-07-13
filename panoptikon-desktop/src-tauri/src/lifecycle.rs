use serde::Serialize;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
pub enum LifecycleState {
    Installing,
    Starting,
    SettingUp,
    Ready,
    #[allow(dead_code)]
    Degraded(String),
    LocalServerDisabled,
    Stopping,
    Failed(String),
    Restarting,
    Exited,
}

impl LifecycleState {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Installing => "Installing",
            Self::Starting => "Starting",
            Self::SettingUp => "Setting up",
            Self::Ready => "Ready",
            Self::Degraded(_) => "Degraded",
            Self::LocalServerDisabled => "Local Server Disabled",
            Self::Stopping => "Stopping",
            Self::Failed(_) => "Failed",
            Self::Restarting => "Restarting",
            Self::Exited => "Exited",
        }
    }
}

#[derive(Debug)]
pub struct RestartBudget {
    exits: VecDeque<Instant>,
    window: Duration,
    stable_reset: Duration,
    max_exits: usize,
}

impl Default for RestartBudget {
    fn default() -> Self {
        Self {
            exits: VecDeque::new(),
            window: Duration::from_secs(300),
            stable_reset: Duration::from_secs(600),
            max_exits: 3,
        }
    }
}

impl RestartBudget {
    pub fn record_exit(&mut self, now: Instant, run_duration: Duration) -> Option<Duration> {
        if run_duration >= self.stable_reset {
            self.exits.clear();
        }
        while self
            .exits
            .front()
            .is_some_and(|at| now.duration_since(*at) > self.window)
        {
            self.exits.pop_front();
        }
        if self.exits.len() >= self.max_exits {
            return None;
        }
        let delay = Duration::from_secs(1 << self.exits.len().min(2));
        self.exits.push_back(now);
        Some(delay)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivationIntent {
    Open,
    Background,
    UpdateRelaunch,
}

pub fn activation_intent(args: &[String]) -> ActivationIntent {
    if args.iter().any(|arg| arg == "--background") {
        ActivationIntent::Background
    } else if args.iter().any(|arg| arg == "--update-relaunch") {
        ActivationIntent::UpdateRelaunch
    } else {
        ActivationIntent::Open
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unexpected exits use 1/2/4-second backoff and the fourth rapid exit is
    /// rejected, preventing an infinite crash loop.
    #[test]
    fn restart_budget_is_bounded() {
        let mut budget = RestartBudget::default();
        let now = Instant::now();
        assert_eq!(
            budget.record_exit(now, Duration::ZERO),
            Some(Duration::from_secs(1))
        );
        assert_eq!(
            budget.record_exit(now + Duration::from_secs(1), Duration::ZERO),
            Some(Duration::from_secs(2))
        );
        assert_eq!(
            budget.record_exit(now + Duration::from_secs(2), Duration::ZERO),
            Some(Duration::from_secs(4))
        );
        assert_eq!(
            budget.record_exit(now + Duration::from_secs(3), Duration::ZERO),
            None
        );
    }

    /// Login and update relaunches stay silent; all other launches request the
    /// state-aware Open action.
    #[test]
    fn activation_routing_is_explicit() {
        assert_eq!(activation_intent(&["app".into()]), ActivationIntent::Open);
        assert_eq!(
            activation_intent(&["app".into(), "--background".into()]),
            ActivationIntent::Background
        );
        assert_eq!(
            activation_intent(&["app".into(), "--update-relaunch".into()]),
            ActivationIntent::UpdateRelaunch
        );
    }
}
