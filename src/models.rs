//! Response types for the Sense REST API.
//!
//! Field names follow the API's wire format. Almost everything is optional
//! because the API omits fields freely. Endpoints that this crate does not
//! model can be reached with
//! [`Client::get_json`](crate::Client::get_json).

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

/// Reads `null` as the default, for fields the API sends as either.
pub(crate) fn null_as_default<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::deserialize(d)?.unwrap_or_default())
}

/// Returned by `POST /authenticate` and `POST /authenticate/mfa`.
///
/// `Debug` output hides the tokens.
#[derive(Clone, Deserialize, Serialize)]
pub struct AuthResponse {
    /// Whether the login was accepted.
    #[serde(default)]
    pub authorized: bool,
    /// The Sense account.
    #[serde(default)]
    pub account_id: i64,
    /// Required: renewing tokens sends it back, so a silent 0 would break renewal later.
    pub user_id: i64,
    /// Bearer token for API requests.
    pub access_token: String,
    /// Token used to get a new access token.
    pub refresh_token: String,
    /// Monitors on the account.
    #[serde(default, deserialize_with = "null_as_default")]
    pub monitors: Vec<Monitor>,
    /// When the account was created (ISO 8601).
    pub date_created: Option<String>,
    /// A/B test group the account is in. Meaning not confirmed; the API is undocumented.
    pub ab_cohort: Option<String>,
    /// Account settings, as raw JSON.
    pub settings: Option<serde_json::Value>,
}

/// Returned by `POST /renew`.
#[derive(Deserialize)]
pub(crate) struct TokenResponse {
    /// Bearer token for API requests.
    pub access_token: String,
    /// Token used to get a new access token.
    pub refresh_token: String,
}

/// Credentials for an authenticated session. Save these to skip logging in again.
///
/// `Debug` output hides the tokens.
#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Session {
    /// Bearer token for API requests.
    pub access_token: String,
    /// Token used to get a new access token.
    pub refresh_token: String,
    /// The logged-in user. Sent back when renewing tokens.
    pub user_id: i64,
    /// Monitor ids returned at login. Empty when restoring a session from tokens alone.
    #[serde(default, deserialize_with = "null_as_default")]
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

/// A Sense monitor installed in an electrical panel.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Monitor {
    /// Monitor ID, passed to most client methods.
    pub id: i64,
    /// Hardware serial number.
    pub serial_number: Option<String>,
    /// IANA time zone name, e.g. `America/Denver`.
    pub time_zone: Option<String>,
    /// Whether the hardware has an ethernet port.
    pub ethernet_supported: Option<bool>,
    /// Whether the auxiliary port is ignored. Meaning not confirmed; the API is undocumented.
    pub aux_ignore: Option<bool>,
    /// What the auxiliary port is used for, e.g. solar. Meaning not confirmed; the API is undocumented.
    pub aux_port: Option<String>,
    /// Hardware model.
    pub hardware_type: Option<String>,
    /// Whether the hardware has a Zigbee radio.
    pub zigbee_supported: Option<bool>,
    /// When the Wi-Fi signal check finished (ISO 8601).
    pub signal_check_completed_time: Option<String>,
}

/// Returned by `GET app/monitors/{id}/overview`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MonitorOverviewResponse {
    /// Changes when the overview changes. Compare with [`DataChange::monitor_overview_checksum`](crate::realtime::DataChange::monitor_overview_checksum).
    pub checksum: Option<String>,
    /// The overview itself.
    pub monitor_overview: MonitorOverview,
}

/// Monitor settings and device counts.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MonitorOverview {
    /// The monitor.
    pub monitor: Option<Monitor>,
    /// Whether native device identification is on. Meaning not confirmed; the API is undocumented.
    pub ndi_enabled: Option<bool>,
    /// Devices Sense has found.
    pub num_devices: Option<i64>,
    /// Devices the user has named.
    pub num_named_devices: Option<i64>,
    /// Devices still without a user-given name.
    pub num_unnamed_devices: Option<i64>,
    /// Utility or partner the monitor came through, if any.
    pub partner_channel: Option<String>,
    /// Partner-specific tags.
    #[serde(default, deserialize_with = "null_as_default")]
    pub partner_tags: Vec<String>,
}

/// Returned by `GET app/monitors/{id}/status`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MonitorStatus {
    /// Network and firmware status.
    pub monitor_info: Option<MonitorStatusInfo>,
    /// Progress of device detection.
    pub device_detection: Option<DeviceDetection>,
}

/// Network and firmware status of a monitor.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MonitorStatusInfo {
    /// Hardware serial number.
    pub serial: Option<String>,
    /// Ethernet MAC address. Meaning not confirmed; the API is undocumented.
    pub emac: Option<String>,
    /// Wi-Fi MAC address.
    pub mac: Option<String>,
    /// Whether the monitor is connected over ethernet.
    pub ethernet: Option<bool>,
    /// Result of the last signal test.
    pub test_result: Option<String>,
    /// Wi-Fi signal strength (dBm).
    pub wifi_strength: Option<i64>,
    /// Whether the monitor is reachable.
    pub online: Option<bool>,
    /// Local IP address.
    pub ip_address: Option<String>,
    /// Firmware version.
    pub version: Option<String>,
    /// Wi-Fi network name.
    pub ssid: Option<String>,
    /// Connection state.
    pub connection_state: Option<ConnectionState>,
}

/// Device detection progress.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeviceDetection {
    /// Devices recently found.
    #[serde(default, deserialize_with = "null_as_default")]
    pub found: Vec<DetectedDevice>,
    /// Devices Sense is still learning.
    #[serde(default, deserialize_with = "null_as_default")]
    pub in_progress: Vec<DetectedDevice>,
    /// Total devices detected.
    pub num_detected: Option<i64>,
}

/// A device in [`DeviceDetection`].
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DetectedDevice {
    /// Icon name.
    pub icon: Option<String>,
    /// Display name.
    pub name: Option<String>,
    /// Detection progress, 0 to 100.
    #[serde(default)]
    pub progress: i64,
}

/// Whether a monitor is connected to Sense.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ConnectionState {
    /// Connecting.
    Connecting,
    /// Connected.
    Online,
    /// Not connected.
    Offline,
}

/// Returned by `GET app/monitors/{id}/devices/overview`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DevicesOverview {
    /// Changes when the device list changes. Compare with [`DataChange::device_data_checksum`](crate::realtime::DataChange::device_data_checksum).
    pub device_data_checksum: Option<String>,
    /// The devices.
    #[serde(default, deserialize_with = "null_as_default")]
    pub devices: Vec<Device>,
}

/// A detected or user-added device. Also used in history `top_movers` and per-device totals.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Device {
    /// Device ID, passed to [`Client::device`](crate::Client::device).
    pub id: Option<String>,
    /// Display name. `None` for devices the user hasn't named.
    pub name: Option<String>,
    /// Manufacturer.
    pub make: Option<String>,
    /// Model.
    pub model: Option<String>,
    /// Where the device is, as entered by the user.
    pub location: Option<String>,
    /// Icon name.
    pub icon: Option<String>,
    /// Free-form device metadata. Values are usually strings, including `"true"`/`"false"`.
    #[serde(default, deserialize_with = "null_as_default")]
    pub tags: HashMap<String, serde_json::Value>,
    /// Share of the total, as a percentage.
    pub pct: Option<f64>,
    /// Usage per step, in kWh.
    #[serde(default, deserialize_with = "null_as_default")]
    pub history: Vec<Option<f64>>,
    /// Cost per step, in cents.
    #[serde(default, deserialize_with = "null_as_default")]
    pub cost_history: Vec<Option<f64>>,
    /// Cents.
    pub total_cost: Option<f64>,
    /// Energy used over the period.
    pub total_kwh: Option<f64>,
    /// How many times it turned on over the period.
    pub num_times_on: Option<i64>,
    /// Time spent on over the period. Meaning not confirmed; the API is undocumented.
    pub total_time_on: Option<f64>,
    /// Change in usage from the previous period, as a percentage.
    pub percent_change: Option<f64>,
    /// Highest power draw seen.
    pub peak_wattage: Option<f64>,
    /// Change in usage from the previous period, in kWh.
    pub kwh_change: Option<f64>,
    /// Change in cost from the previous period, in cents.
    pub cost_change_cents: Option<f64>,
}

/// Time range bucket for history endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scale {
    /// One day.
    Day,
    /// One week.
    Week,
    /// One calendar month.
    Month,
    /// One calendar year.
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
    /// One sample per second.
    Second,
    /// One sample per minute.
    Minute,
    /// One sample per hour.
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
    /// Number of steps in the period, e.g. 24 for a day.
    pub steps: Option<i64>,
    /// The requested [`Scale`].
    pub scale: Option<String>,
    /// Start of the period (ISO 8601).
    pub start: Option<String>,
    /// End of the period (ISO 8601).
    pub end: Option<String>,
    /// Human-readable summary of the trend.
    pub trend_text: Option<String>,
    /// Human-readable summary of usage.
    pub usage_text: Option<String>,
    /// Energy used.
    pub consumption: Option<EnergyTotals>,
    /// Energy produced, e.g. by solar.
    pub production: Option<EnergyTotals>,
    /// Change in consumption from the previous period, as a percentage.
    pub consumption_percent_change: Option<f64>,
    /// Change in production from the previous period, as a percentage.
    pub production_percent_change: Option<f64>,
    /// Change in consumption from the previous period, in kWh.
    pub consumption_kwh_change: Option<f64>,
    /// Change in consumption cost from the previous period, in cents.
    pub consumption_cost_change_cents: Option<f64>,
    /// Energy drawn from the grid, in kWh.
    pub from_grid: Option<f64>,
    /// Cost of energy drawn from the grid.
    pub from_grid_cost: Option<f64>,
    /// Energy sent to the grid, in kWh.
    pub to_grid: Option<f64>,
    /// Value of energy sent to the grid.
    pub to_grid_cost: Option<f64>,
    /// Share of consumption covered by solar, as a percentage. Meaning not confirmed; the API is undocumented.
    pub solar_powered: Option<f64>,
    /// Production minus consumption, in kWh. Meaning not confirmed; the API is undocumented.
    pub net_production: Option<f64>,
    /// Production as a percentage of consumption. Meaning not confirmed; the API is undocumented.
    pub production_pct: Option<i64>,
    /// Energy from the grid into a battery.
    pub grid_to_battery: Option<EnergyFlow>,
    /// Energy from a battery into the home.
    pub battery_to_home: Option<EnergyFlow>,
    /// Energy from solar into a battery.
    pub solar_to_battery: Option<EnergyFlow>,
    /// Energy from a battery into the grid.
    pub battery_to_grid: Option<EnergyFlow>,
    /// Energy from solar into the home.
    pub solar_to_home: Option<EnergyFlow>,
    /// Carbon intensity data, as raw JSON.
    pub carbon_intensity: Option<serde_json::Value>,
    /// Devices whose usage changed most from the previous period.
    #[serde(default, deserialize_with = "null_as_default")]
    pub top_movers: Vec<Device>,
}

/// Energy and cost totals over a period.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EnergyTotals {
    /// kWh.
    #[serde(default)]
    pub total: f64,
    /// Cents.
    pub total_cost: Option<f64>,
    /// kWh per step.
    #[serde(default, deserialize_with = "null_as_default")]
    pub totals: Vec<Option<f64>>,
    /// Cents per step.
    #[serde(default, deserialize_with = "null_as_default")]
    pub total_costs: Vec<Option<f64>>,
    /// Per-device totals.
    #[serde(default, deserialize_with = "null_as_default")]
    pub devices: Vec<Device>,
}

/// Energy moved between two places, e.g. solar to battery.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EnergyFlow {
    /// Energy moved, in kWh.
    pub total_kwh: Option<f64>,
    /// Cents.
    pub cost: Option<f64>,
}

/// Returned by `GET app/history/usage`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Usage {
    /// Time of the first sample (ISO 8601).
    pub start: Option<String>,
    /// Time of the last available sample (ISO 8601).
    pub end_of_data: Option<String>,
    /// Samples, one inner list per frame. Meaning not confirmed; the API is undocumented.
    #[serde(default, deserialize_with = "null_as_default")]
    pub totals: Vec<Vec<Option<f64>>>,
}

/// Returned by `GET app/history/cumulative`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CumulativeUsage {
    /// Start of the billing cycle (ISO 8601).
    pub cycle_start: Option<String>,
    /// End of the billing cycle (ISO 8601).
    pub cycle_end: Option<String>,
    /// Start of the data (ISO 8601).
    pub start: Option<String>,
    /// End of the data (ISO 8601).
    pub end: Option<String>,
    /// Number of steps in the full cycle.
    #[serde(default)]
    pub max_steps: i64,
    /// Energy used so far.
    pub consumption: Option<EnergyTotals>,
    /// Projected consumption for the whole cycle, in kWh. Meaning not confirmed; the API is undocumented.
    pub trend_consumption: Option<f64>,
}

/// Error body returned by the API on failure.
#[derive(Debug, Deserialize)]
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
                "grid_to_battery": {"total_kwh": 1.0, "cost": 2.5},
                "top_movers": null
            }"#,
        )
        .unwrap();
        let consumption = trends.consumption.unwrap();
        assert_eq!(consumption.total_cost, Some(412.5));
        assert_eq!(consumption.totals, vec![Some(0.5), Some(1.0), None]);
        assert_eq!(consumption.total_costs, vec![Some(10.25), None]);
        assert_eq!(consumption.devices[0].cost_history, vec![Some(0.5), None]);
        assert_eq!(consumption.devices[0].name, None);
        assert!(trends.top_movers.is_empty());
    }

    #[test]
    fn null_lists_decode_as_empty() {
        let overview: DevicesOverview = serde_json::from_str(r#"{"devices":null}"#).unwrap();
        assert!(overview.devices.is_empty());
    }
}
