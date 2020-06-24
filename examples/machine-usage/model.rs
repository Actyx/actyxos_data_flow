use abomonation_derive::Abomonation;
use actyxos_data_flow::db::{DbColumn, DbMechanics, DbRecord, SqliteDbMechanics};
use actyxos_sdk::event::TimeStamp;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Abomonation)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum MachineEvent {
    Started { order: String },
    Stopped { order: String },
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Abomonation)]
pub struct UsageEntry {
    pub machine: String,
    pub order: String,
    pub started: TimeStamp,
    pub duration_micros: i64,
}

impl DbRecord<SqliteDbMechanics> for UsageEntry {
    fn table_version() -> i32 {
        1
    }
    fn table_name() -> &'static str {
        "usage"
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
                name: "manufacturing_order",
                tpe: "text",
                exclude: false,
                index: false,
            },
            DbColumn {
                name: "started",
                tpe: "timestamp with time zone",
                exclude: false,
                index: true,
            },
            DbColumn {
                name: "duration_micros",
                tpe: "bigint",
                exclude: false,
                index: false,
            },
        ];
        X
    }
    fn values(&self) -> Vec<<SqliteDbMechanics as DbMechanics>::SqlValue> {
        vec![
            Box::new(self.machine.clone()),
            Box::new(self.order.clone()),
            Box::new(self.started.as_i64() / 1_000_000),
            Box::new(self.duration_micros),
        ]
    }
}
