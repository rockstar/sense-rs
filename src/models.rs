//! Response types for the Sense REST API.
//!
//! Field names follow the API's wire format. Almost everything is optional
//! because the API omits fields freely. Endpoints that this crate does not
//! model can be reached with
//! [`Client::get_json`](crate::Client::get_json).

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

/// Returned by `POST /authenticate` and `POST /authenticate/mfa`.
///
/// `Debug` output hides the tokens.
#[derive(Clone, Deserialize, Serialize)]
pub struct AuthResponse {
    #[serde(default)]
    pub authorized: bool,
    #[serde(default)]
    pub account_id: i64,
    #[serde(default)]
    pub user_id: i64,
    pub access_token: String,
    pub refresh_token: String,
    #[serde(default)]
    pub monitors: Vec<Monitor>,
    pub date_created: Option<String>,
    pub ab_cohort: Option<String>,
    pub settings: Option<serde_json::Value>,
}

/// Returned by `POST /renew`.
#[derive(Deserialize)]
pub(crate) struct TokenResponse {
    pub access_token: String,
    pub refresh_token: String,
}

/// Credentials for an authenticated session. Save these to skip logging in again.
///
/// `Debug` output hides the tokens.
#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Session {
    pub access_token: String,
    pub refresh_token: String,
    pub user_id: i64,
    /// Monitor ids returned at login. Empty when restoring a session from tokens alone.
    #[serde(default)]
    pub monitor_ids: Vec<i64>,
}

const REDACTED: &str = "<redacted>";

impl fmt::Debug for AuthResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthResponse")
            .field("authorized", &self.authorized)
            .field("account_id", &self.account_id)
            .field("user_id", &self.user_id)
            .field("access_token", &REDACTED)
            .field("refresh_token", &REDACTED)
            .field("monitors", &self.monitors)
            .field("date_created", &self.date_created)
            .field("ab_cohort", &self.ab_cohort)
            .field("settings", &self.settings)
            .finish()
    }
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("access_token", &REDACTED)
            .field("refresh_token", &REDACTED)
            .field("user_id", &self.user_id)
            .field("monitor_ids", &self.monitor_ids)
            .finish()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Monitor {
    pub id: i64,
    pub serial_number: Option<String>,
    pub time_zone: Option<String>,
    pub ethernet_supported: Option<bool>,
    pub aux_ignore: Option<bool>,
    pub aux_port: Option<String>,
    pub hardware_type: Option<String>,
    pub zigbee_supported: Option<bool>,
    pub signal_check_completed_time: Option<String>,
}

/// Returned by `GET app/monitors/{id}/overview`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MonitorOverviewResponse {
    pub checksum: Option<String>,
    pub monitor_overview: MonitorOverview,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MonitorOverview {
    pub monitor: Option<Monitor>,
    pub ndi_enabled: Option<bool>,
    pub num_devices: Option<i64>,
    pub num_named_devices: Option<i64>,
    pub num_unnamed_devices: Option<i64>,
    pub partner_channel: Option<String>,
    #[serde(default)]
    pub partner_tags: Vec<String>,
}

/// Returned by `GET app/monitors/{id}/status`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MonitorStatus {
    pub monitor_info: Option<MonitorStatusInfo>,
    pub device_detection: Option<DeviceDetection>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MonitorStatusInfo {
    pub serial: Option<String>,
    pub emac: Option<String>,
    pub mac: Option<String>,
    pub ethernet: Option<bool>,
    pub test_result: Option<String>,
    pub wifi_strength: Option<i64>,
    pub online: Option<bool>,
    pub ip_address: Option<String>,
    pub version: Option<String>,
    pub ssid: Option<String>,
    pub connection_state: Option<ConnectionState>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeviceDetection {
    #[serde(default)]
    pub found: Vec<DetectedDevice>,
    #[serde(default)]
    pub in_progress: Vec<DetectedDevice>,
    pub num_detected: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DetectedDevice {
    pub icon: Option<String>,
    pub name: Option<String>,
    #[serde(default)]
    pub progress: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ConnectionState {
    Connecting,
    Online,
    Offline,
}

/// Returned by `GET app/monitors/{id}/devices/overview`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DevicesOverview {
    pub device_data_checksum: Option<String>,
    #[serde(default)]
    pub devices: Vec<Device>,
}

/// A detected or user-added device. Also used in history `top_movers` and per-device totals.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Device {
    pub id: Option<String>,
    pub name: Option<String>,
    pub make: Option<String>,
    pub model: Option<String>,
    pub location: Option<String>,
    pub icon: Option<String>,
    /// Free-form device metadata. Values are usually strings, including `"true"`/`"false"`.
    #[serde(default)]
    pub tags: HashMap<String, serde_json::Value>,
    pub pct: Option<f64>,
    #[serde(default)]
    pub history: Vec<Option<f64>>,
    #[serde(default)]
    pub cost_history: Vec<Option<f64>>,
    /// Cents.
    pub total_cost: Option<f64>,
    pub total_kwh: Option<f64>,
    pub num_times_on: Option<i64>,
    pub total_time_on: Option<f64>,
    pub percent_change: Option<f64>,
    pub peak_wattage: Option<f64>,
    pub kwh_change: Option<f64>,
    pub cost_change_cents: Option<f64>,
}

/// Time range bucket for history endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scale {
    Day,
    Week,
    Month,
    Year,
    /// The current billing cycle.
    Cycle,
}

impl fmt::Display for Scale {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Scale::Day => "DAY",
            Scale::Week => "WEEK",
            Scale::Month => "MONTH",
            Scale::Year => "YEAR",
            Scale::Cycle => "CYCLE",
        })
    }
}

/// Sample size for [`Client::usage`](crate::Client::usage).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Granularity {
    Second,
    Minute,
    Hour,
}

impl fmt::Display for Granularity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Granularity::Second => "second",
            Granularity::Minute => "minute",
            Granularity::Hour => "hour",
        })
    }
}

/// Returned by `GET app/history/trends`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Trends {
    pub steps: Option<i64>,
    pub scale: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub trend_text: Option<String>,
    pub usage_text: Option<String>,
    pub consumption: Option<EnergyTotals>,
    pub production: Option<EnergyTotals>,
    pub consumption_percent_change: Option<f64>,
    pub production_percent_change: Option<f64>,
    pub consumption_kwh_change: Option<f64>,
    pub consumption_cost_change_cents: Option<f64>,
    pub from_grid: Option<f64>,
    pub from_grid_cost: Option<f64>,
    pub to_grid: Option<f64>,
    pub to_grid_cost: Option<f64>,
    pub solar_powered: Option<f64>,
    pub net_production: Option<f64>,
    pub production_pct: Option<i64>,
    pub grid_to_battery: Option<EnergyFlow>,
    pub battery_to_home: Option<EnergyFlow>,
    pub solar_to_battery: Option<EnergyFlow>,
    pub battery_to_grid: Option<EnergyFlow>,
    pub solar_to_home: Option<EnergyFlow>,
    pub carbon_intensity: Option<serde_json::Value>,
    #[serde(default)]
    pub top_movers: Vec<Device>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EnergyTotals {
    /// kWh.
    #[serde(default)]
    pub total: f64,
    /// Cents.
    pub total_cost: Option<f64>,
    /// kWh per step.
    #[serde(default)]
    pub totals: Vec<Option<f64>>,
    #[serde(default)]
    pub total_costs: Vec<Option<f64>>,
    #[serde(default)]
    pub devices: Vec<Device>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EnergyFlow {
    pub total_kwh: Option<f64>,
    /// Cents.
    pub cost: Option<f64>,
}

/// Returned by `GET app/history/usage`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Usage {
    pub start: Option<String>,
    pub end_of_data: Option<String>,
    #[serde(default)]
    pub totals: Vec<Vec<Option<f64>>>,
}

/// Returned by `GET app/history/cumulative`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CumulativeUsage {
    pub cycle_start: Option<String>,
    pub cycle_end: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    #[serde(default)]
    pub max_steps: i64,
    pub consumption: Option<EnergyTotals>,
    pub trend_consumption: Option<f64>,
}

/// Error body returned by the API on failure.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ApiErrorBody {
    pub status: Option<String>,
    pub error_reason: Option<String>,
    pub mfa_token: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_hides_tokens() {
        let session = Session {
            access_token: "secret-access".into(),
            refresh_token: "secret-refresh".into(),
            user_id: 7,
            monitor_ids: vec![1],
        };
        let auth: AuthResponse = serde_json::from_str(
            r#"{"access_token":"secret-access","refresh_token":"secret-refresh","user_id":7}"#,
        )
        .unwrap();
        for out in [format!("{session:?}"), format!("{auth:?}")] {
            assert!(!out.contains("secret"), "{out}");
            assert!(out.contains("user_id: 7"), "{out}");
        }
    }

    #[test]
    fn trends_tolerates_fractional_costs_nulls_and_unnamed_devices() {
        let trends: Trends = serde_json::from_str(
            r#"{
                "consumption_cost_change_cents": -3.5,
                "consumption": {
                    "total": 1.5,
                    "total_cost": 412.5,
                    "totals": [0.5, 1.0, null],
                    "total_costs": [10.25, null],
                    "devices": [{"id": "x", "total_cost": 1.5, "cost_history": [0.5, null]}]
                },
                "grid_to_battery": {"total_kwh": 1.0, "cost": 2.5}
            }"#,
        )
        .unwrap();
        let consumption = trends.consumption.unwrap();
        assert_eq!(consumption.total_cost, Some(412.5));
        assert_eq!(consumption.totals, vec![Some(0.5), Some(1.0), None]);
        assert_eq!(consumption.total_costs, vec![Some(10.25), None]);
        assert_eq!(consumption.devices[0].cost_history, vec![Some(0.5), None]);
        assert_eq!(consumption.devices[0].name, None);
    }
}
