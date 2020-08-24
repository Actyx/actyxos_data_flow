use abomonation_derive::Abomonation;
use actyxos_data_flow::db::{DbColumn, DbMechanics, DbRecord, SqliteDbMechanics};
use serde::{Deserialize, Serialize};
use actyxos_sdk::types::ArcVal;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Abomonation)]
pub struct FinishedGoods {
    pub article_id: ArcVal<str>,
    pub article_name: ArcVal<str>,
    pub workstation: ArcVal<str>,
    pub order_id: ArcVal<str>,
    pub pcs: i64,
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Abomonation)]
pub struct ProductionSummary {
    pub article_id: ArcVal<str>,
    pub article_name: ArcVal<str>,
    pub workstation: ArcVal<str>,
    pub total_pcs: i64,
}

impl DbRecord<SqliteDbMechanics> for ProductionSummary {
    fn table_version() -> i32 {
        1
    }
    fn table_name() -> &'static str {
        "production_summary"
    }
    fn columns() -> &'static [actyxos_data_flow::db::DbColumn] {
        static X: &[DbColumn] = &[
            DbColumn {
                name: "article_id",
                tpe: "text not null",
                exclude: false,
                index: true,
            },
            DbColumn {
                name: "article_name",
                tpe: "text not null",
                exclude: false,
                index: true,
            },
            DbColumn {
                name: "workstation",
                tpe: "text not null",
                exclude: false,
                index: true,
            },
            DbColumn {
                name: "total_pcs",
                tpe: "bigint",
                exclude: false,
                index: false,
            },
        ];
        X
    }
    fn values(&self) -> Vec<<SqliteDbMechanics as DbMechanics>::SqlValue> {
        vec![
            Box::new(self.article_id.to_string()),
            Box::new(self.article_name.to_string()),
            Box::new(self.workstation.to_string()),
            Box::new(self.total_pcs),
        ]
    }
}
