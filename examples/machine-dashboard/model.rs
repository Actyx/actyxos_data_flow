use abomonation_derive::Abomonation;
use actyxos_data_flow::db::{DbColumn, DbMechanics, DbRecord, SqliteDbMechanics};
use actyxos_sdk::event::TimeStamp;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Abomonation)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum MachineEvent {
    Started { order: String },
    Stopped { order: String },
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Abomonation)]
pub struct DashboardEntry {
    pub machine: String,
    pub status: MachineStatus,
    pub since: TimeStamp,
}

impl DashboardEntry {
    pub fn idle(machine: String, since: TimeStamp) -> Self {
        Self {
            machine,
            status: MachineStatus::Idle,
            since,
        }
    }
    pub fn working(machine: String, order: String, since: TimeStamp) -> Self {
        Self {
            machine,
            status: MachineStatus::WorkingOn(order),
            since,
        }
    }
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Abomonation)]
pub enum MachineStatus {
    Idle,
    WorkingOn(String),
}

impl MachineStatus {
    pub fn state(&self) -> &'static str {
        match self {
            MachineStatus::Idle => "idle",
            MachineStatus::WorkingOn(_) => "working",
        }
    }
    pub fn order(&self) -> Option<String> {
        match self {
            MachineStatus::Idle => None,
            MachineStatus::WorkingOn(order) => Some(order.clone()),
        }
    }
}

impl DbRecord<SqliteDbMechanics> for DashboardEntry {
    fn table_version() -> i32 {
        1
    }
    fn table_name() -> &'static str {
        "dashboard"
    }
    fn columns() -> &'static [actyxos_data_flow::db::DbColumn] {
        static X: &[DbColumn] = &[
            DbColumn {
                name: "machine",
                tpe: "text not null",
                exclude: false,
                index: true,
            },
            DbColumn {
                name: "status",
                tpe: "text not null",
                exclude: false,
                index: false,
            },
            DbColumn {
                name: "manufacturing_order",
                tpe: "text",
                exclude: false,
                index: false,
            },
            DbColumn {
                name: "since",
                tpe: "timestamp with time zone",
                exclude: false,
                index: false,
            },
        ];
        X
    }
    fn values(&self) -> Vec<<SqliteDbMechanics as DbMechanics>::SqlValue> {
        vec![
            Box::new(self.machine.clone()),
            Box::new(self.status.state()),
            Box::new(self.status.order()),
            Box::new(ts(self.since).timestamp()),
        ]
    }
}

fn ts(t: TimeStamp) -> DateTime<Utc> {
    t.into()
}
