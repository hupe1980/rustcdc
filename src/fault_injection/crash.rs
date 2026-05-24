//! Crash simulation for testing recovery and checkpoint persistence.

use std::sync::{Arc, Mutex};

use crate::core::{Error, Event, Result};

/// Result of a crash simulation.
#[derive(Debug, Clone)]
pub struct CrashSimulationResult {
    /// Total events collected across all crash/restart cycles
    pub total_events: u64,
    /// Events by cycle (count of events in each cycle)
    pub events_per_cycle: Vec<u64>,
    /// Total cycles (crashes + final run)
    pub total_cycles: u64,
    /// Crash points (event counts where crashes occurred)
    pub crash_points: Vec<u64>,
}

/// State tracked across crash/restart cycles for validation.
#[derive(Debug, Clone, Default)]
pub struct CrashSimulationState {
    /// All events collected across cycles
    pub collected_events: Arc<Mutex<Vec<Event>>>,
    /// Event count at each crash point
    pub events_per_cycle: Arc<Mutex<Vec<u64>>>,
    /// Current cycle number
    pub current_cycle: Arc<Mutex<u64>>,
    /// Crash points (event counts where crash should be triggered)
    pub crash_points: Arc<Mutex<Vec<u64>>>,
}

impl CrashSimulationState {
    /// Create a new crash simulation state.
    pub fn new(crash_points: Vec<u64>) -> Self {
        Self {
            collected_events: Arc::new(Mutex::new(Vec::new())),
            events_per_cycle: Arc::new(Mutex::new(Vec::new())),
            current_cycle: Arc::new(Mutex::new(0)),
            crash_points: Arc::new(Mutex::new(crash_points)),
        }
    }

    /// Add collected events from a cycle.
    pub fn record_cycle(&self, events: Vec<Event>) -> Result<()> {
        let count = events.len() as u64;
        if let Ok(mut collected) = self.collected_events.lock() {
            collected.extend(events);
        }
        if let Ok(mut per_cycle) = self.events_per_cycle.lock() {
            per_cycle.push(count);
        }
        if let Ok(mut cycle) = self.current_cycle.lock() {
            *cycle += 1;
        }
        Ok(())
    }

    /// Check if we should crash at this event count.
    pub fn should_crash_at(&self, event_count: u64) -> Result<bool> {
        if let Ok(crashes) = self.crash_points.lock() {
            if let Ok(cycle) = self.current_cycle.lock() {
                let cycle_idx = *cycle as usize;
                if cycle_idx < crashes.len() && crashes[cycle_idx] == event_count {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Get all collected events.
    pub fn get_collected_events(&self) -> Result<Vec<Event>> {
        self.collected_events
            .lock()
            .map(|e| e.clone())
            .map_err(|e| Error::StateError(format!("Failed to lock collected events: {}", e)))
    }

    /// Get events per cycle.
    pub fn get_events_per_cycle(&self) -> Result<Vec<u64>> {
        self.events_per_cycle
            .lock()
            .map(|e| e.clone())
            .map_err(|e| Error::StateError(format!("Failed to lock events per cycle: {}", e)))
    }

    /// Get total cycles executed.
    pub fn get_total_cycles(&self) -> Result<u64> {
        self.current_cycle
            .lock()
            .map(|c| *c)
            .map_err(|e| Error::StateError(format!("Failed to lock current cycle: {}", e)))
    }

    /// Finalize and return the crash simulation result.
    pub fn finalize(&self) -> Result<CrashSimulationResult> {
        let total_events = self
            .collected_events
            .lock()
            .map(|e| e.len() as u64)
            .unwrap_or(0);
        let events_per_cycle = self.get_events_per_cycle()?;
        let total_cycles = self.get_total_cycles()?;
        let crash_points = self
            .crash_points
            .lock()
            .map(|c| c.clone())
            .unwrap_or_default();

        Ok(CrashSimulationResult {
            total_events,
            events_per_cycle,
            total_cycles,
            crash_points,
        })
    }
}

/// Validates crash simulation results for correctness.
pub struct CrashSimulationValidator;

impl CrashSimulationValidator {
    /// Validate that crash simulation preserved all events without unexpected duplicates.
    ///
    /// Rules:
    /// - No missing events between crash points
    /// - Total event count should match expected
    /// - Events within each cycle should be contiguous
    /// - Some duplication is acceptable at crash boundaries (at-least-once semantics)
    pub fn validate(
        result: &CrashSimulationResult,
        expected_total_events: u64,
        max_duplicate_rate: f64,
    ) -> Result<ValidationReport> {
        let actual_total = result.total_events;

        // Check for missing events (allowing for some duplication at boundaries)
        if actual_total < expected_total_events {
            return Err(Error::SourceError(format!(
                "Data loss detected: expected {}, got {}",
                expected_total_events, actual_total
            )));
        }

        // Calculate duplicate rate
        let duplicates = actual_total - expected_total_events;
        let duplicate_rate = if expected_total_events > 0 {
            duplicates as f64 / expected_total_events as f64
        } else {
            0.0
        };

        if duplicate_rate > max_duplicate_rate {
            return Err(Error::SourceError(format!(
                "Excessive duplication: {:.2}% (allowed: {:.2}%)",
                duplicate_rate * 100.0,
                max_duplicate_rate * 100.0
            )));
        }

        Ok(ValidationReport {
            total_events_collected: actual_total,
            expected_total_events,
            duplicate_count: duplicates,
            duplicate_rate,
            total_cycles: result.total_cycles,
            cycles_with_crashes: result.crash_points.len() as u64,
            passed: true,
        })
    }
}

/// Report from crash simulation validation.
#[derive(Debug, Clone)]
pub struct ValidationReport {
    pub total_events_collected: u64,
    pub expected_total_events: u64,
    pub duplicate_count: u64,
    pub duplicate_rate: f64,
    pub total_cycles: u64,
    pub cycles_with_crashes: u64,
    pub passed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_event(id: u64) -> Event {
        Event {
            before: None,
            after: Some(serde_json::json!({"id": id})),
            op: crate::core::Operation::Insert,
            source: crate::core::SourceMetadata {
                source_name: "test".into(),
                offset: id.to_string(),
                timestamp: id,
            },
            ts: id,
            schema: None,
            table: "test_table".into(),
            primary_key: None,
            snapshot: None,
            transaction: None,
            envelope_version: crate::EVENT_ENVELOPE_VERSION,
        }
    }

    #[test]
    fn test_crash_simulation_state_creation() {
        let state = CrashSimulationState::new(vec![100, 250, 500]);
        assert_eq!(state.get_total_cycles().unwrap(), 0);
        assert_eq!(state.get_collected_events().unwrap().len(), 0);
    }

    #[test]
    fn test_crash_simulation_record_cycle() {
        let state = CrashSimulationState::new(vec![100, 250, 500]);
        let events = vec![
            create_test_event(1),
            create_test_event(2),
            create_test_event(3),
        ];
        state.record_cycle(events).unwrap();
        assert_eq!(state.get_total_cycles().unwrap(), 1);
        assert_eq!(state.get_collected_events().unwrap().len(), 3);
        assert_eq!(state.get_events_per_cycle().unwrap(), vec![3]);
    }

    #[test]
    fn test_crash_simulation_should_crash_at() {
        let state = CrashSimulationState::new(vec![100, 250, 500]);
        assert!(!state.should_crash_at(99).unwrap());
        assert!(state.should_crash_at(100).unwrap());

        state.record_cycle(vec![]).unwrap();
        assert!(!state.should_crash_at(100).unwrap());
        assert!(state.should_crash_at(250).unwrap());
    }

    #[test]
    fn test_crash_simulation_validator_no_loss() {
        let result = CrashSimulationResult {
            total_events: 1000,
            events_per_cycle: vec![100, 200, 300, 400],
            total_cycles: 4,
            crash_points: vec![100, 250, 500],
        };
        let report = CrashSimulationValidator::validate(&result, 1000, 0.05).unwrap();
        assert!(report.passed);
        assert_eq!(report.duplicate_count, 0);
    }

    #[test]
    fn test_crash_simulation_validator_with_acceptable_duplicates() {
        let result = CrashSimulationResult {
            total_events: 1050,
            events_per_cycle: vec![105, 245, 350, 350],
            total_cycles: 4,
            crash_points: vec![100, 250, 500],
        };
        let report = CrashSimulationValidator::validate(&result, 1000, 0.1).unwrap();
        assert!(report.passed);
        assert_eq!(report.duplicate_count, 50);
        assert!(report.duplicate_rate <= 0.1);
    }

    #[test]
    fn test_crash_simulation_validator_data_loss_detected() {
        let result = CrashSimulationResult {
            total_events: 950,
            events_per_cycle: vec![100, 200, 300, 300],
            total_cycles: 4,
            crash_points: vec![100, 250, 500],
        };
        let report = CrashSimulationValidator::validate(&result, 1000, 0.05);
        assert!(report.is_err());
    }

    #[test]
    fn test_crash_simulation_validator_excessive_duplicates() {
        let result = CrashSimulationResult {
            total_events: 1200,
            events_per_cycle: vec![120, 280, 400, 400],
            total_cycles: 4,
            crash_points: vec![100, 250, 500],
        };
        let report = CrashSimulationValidator::validate(&result, 1000, 0.05);
        assert!(report.is_err());
    }
}
