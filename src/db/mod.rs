//! Tools for writing collection deltas and event offsets into SQL databases
//!
//! The core is the [`DB`](struct.DB.html) trait, which represents a set of database
//! tables for some record types (the `Record` associated type) within some database
//! type (the `Mechanics` associated type). With such a `DB` implementation we
//! can
//!
//!  - get the previously stored offset map for this set of tables
//!  - update these tables with new deltas and store the corresponding offset map
//!    within the same database transaction
//!
//! Each table that participates in this consists of records of some type (usually
//! a `struct` with one field per column) and an instance of the `DbRecord` trait
//! for this type. Multiple tables can be updated together using the `Union` type
//! that also implements the `DbRecordExt` trait needed by the database driver to
//! construct the right SQL queries.
//!
//! This scheme is extensible, you can add your own database drivers by implementing
//! the traits, please see the existing drivers for examples of how to use them.
//!
//! # Data storage format
//!
//! Each table’s name is defined by the [`table_name()`](trait.DbRecord.html#method.table_name)
//! attribute. Each set of tables is accompanied by one table that ends with the
//! [`OFFSETS_TABLE_SUFFIX`](constant.OFFSETS_TABLE_SUFFIX.html), holding the offset
//! map describing all events that have led to the current state of the set of tables.
//!
//! # Database schema migrations
//!
//! Since the database tables are derived from persistent events, no effort is made
//! to massage existing rows into a new format when the record type changes (as is
//! signaled by incrementing the [`table_version()`](trait.DbRecord.html#method.table_version)).
//! The previously used table version is stored in a table whose name is defined in
//! [`SCHEMA_VERSIONS_TABLE_NAME`](constant.SCHEMA_VERSIONS_TABLE_NAME.html).
//!
//! Thus, when the target table has the wrong version it is dropped and created anew,
//! then to be filled with the correct data in the new format. The previously stored
//! offset map is also removed and repopulated while rebuilding the table.

use abomonation_derive::Abomonation;
use actyxos_sdk::event::OffsetMap;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

mod mssql;
mod postgre;
mod sqlite;

pub use mssql::{MssqlDB, MssqlDbMechanics};
pub use postgre::{PostgresDB, PostgresDbMechanics};
pub use sqlite::{SqliteDB, SqliteDbMechanics};

pub const SCHEMA_VERSIONS_TABLE_NAME: &str = "_schema_versions";
pub const OFFSETS_TABLE_SUFFIX: &str = "offsets";

/// Functions and types needed for constructing the SQL queries for a database driver
pub trait DbMechanics {
    /// the type into which each record value must be converted before inserting into the database
    ///
    /// This defines the return type of the [`values()`](trait.DbRecord.html#method.values)
    /// function that each record’s [`DbRecord`](trait.DbRecord.html) instance needs to provide.
    type SqlValue;

    /// A common prefix that is prepended to all table names within this database
    fn table_prefix(&self) -> &str;
    /// A function that computes a final table name; by default prepends the [`table_prefix`](#method.table_prefix)
    fn table_name(&self, table: &str) -> String {
        format!("{}{}", self.table_prefix(), table)
    }

    /// An iterator emitting parameter placeholder strings for use in prepared statements
    fn field_iter() -> Box<dyn Iterator<Item = String>>;
    /// Compute the SQL statement necessary to create the given table if it does not yet exist
    fn create_table(name: String, definition: String) -> String;
    /// Compute the SQL statement necessary to create the given index if it does not yet exist
    fn create_index(name: String, table: String, definition: String) -> String;
    /// Compute the SQL statement necessary to remove a number of copies of a row
    ///
    /// The number should be taken as the next placeholder emitted by the `fields` iterator
    /// and then supplied by the code implementing [`advance_offsets`](trait.DB.html#method.advance_offsets).
    fn delete_limit(clause: String, fields: Box<dyn Iterator<Item = String>>) -> String;
    /// Compute the SQL statement necessary to compare the value of the named column to the
    /// given value
    ///
    /// This is complicated by the fact that NULL usually cannot be compared like other values,
    /// see the postgres driver for an example.
    fn compare_value(column: &str, value: String) -> String;
}

/// Representation of a set of database tables in a certain kind of database
pub trait DB {
    /// required type for which the record needs to provide suitable column types and values
    type Mechanics: DbMechanics;
    /// type of record for which this database driver is responsible
    type Record: 'static + DbRecordExt<Self::Mechanics>;

    /// read the offset map up to which events have been included in the currently stored tables
    fn get_offsets(&mut self) -> Result<OffsetMap>;
    /// store new offset map and the deltas corresponding to the set of events now added to the offset map
    fn advance_offsets<C>(&mut self, offsets: &OffsetMap, deltas: C) -> Result<()>
    where
        C: IntoIterator<Item = (Self::Record, isize)>;
}

/// representation of a database column for use in a DbRecord
pub struct DbColumn {
    /// name of the column to be used; must be suitable for the database as is, no quoting is done
    pub name: &'static str,
    /// data type stored by this column as is suitable for a CREATE TABLE statement
    pub tpe: &'static str,
    /// if true, this column does not receive a value when writing records (e.g. AUTO_INCREMENT)
    pub exclude: bool,
    /// if true, this column shall be part of the index created automatically by the driver
    ///
    /// If all columns set this to false, no index will be created. The index does not assume
    /// uniqueness. Columns should be ordered such that the index will be most effective.
    pub index: bool,
}

/// representation of a database table for some record type
pub trait DbRecord<D: DbMechanics>: Sized {
    /// version number for the format
    ///
    /// It is good practice to start with 1 and increment whenver the contents of the table should
    /// be recomputed (e.g. a logic change or a change to the column definitions).
    fn table_version() -> i32;
    /// name of the table
    ///
    /// If the database has been configured to use a common [`table_prefix()`](struct.DbMechanics.html#method.table_prefix)
    /// then that is prepended to this name to obtain the final name used in the database.
    ///
    /// ```rust
    /// fn table_name() -> &'static str {
    ///     "my_table"
    /// }
    /// ```
    fn table_name() -> &'static str;
    /// columns of this table
    ///
    /// Note that this intentionally has a return type that cannot be implemented dynamically.
    ///
    /// ```rust
    /// # use actyxos_data_flow::db::DbColumn;
    /// fn columns() -> &'static [DbColumn] {
    ///     static X: &[DbColumn] = &[
    ///         DbColumn { name: "x", tpe: "bigint not null", exclude: false, index: true }
    ///     ];
    ///     X
    /// }
    /// ```
    fn columns() -> &'static [DbColumn];
    /// values to fill the non-exluded columns as defined by the `columns` function
    ///
    /// ```rust
    /// use actyxos_data_flow::db::{DbColumn, DbMechanics, DbRecord, SqliteDbMechanics};
    ///
    /// struct X { x: i64 }
    ///
    /// impl DbRecord<SqliteDbMechanics> for X {
    ///     fn table_version() -> i32 { 1 }
    ///     fn table_name() -> &'static str { "my_table" }
    ///     fn columns() -> &'static [DbColumn] {
    ///         static X: &[DbColumn] = &[
    ///             DbColumn { name: "x", tpe: "bigint not null", exclude: false, index: true }
    ///         ];
    ///         X
    ///     }
    ///     fn values(&self) -> Vec<<SqliteDbMechanics as DbMechanics>::SqlValue> {
    ///         vec![Box::new(self.x)]
    ///     }
    /// }
    /// ```
    ///
    /// It is important to ensure that the values returned are fully consistent with all non-excluded
    /// column descriptions returned by the [`columns`](#method.columns) method, they must appear in
    /// the same order.
    fn values(&self) -> Vec<D::SqlValue>;
}

/// Extension trait automatically defined for a set of database tables
///
/// The set is formed using the [`Union`](enum.Union.html) enum. Data of a single record can
/// be lifted into a union using the provided `From` instance. The methods provided by this
/// trait are used by the [`DB`](trait.DB.html) implementation. They are based on the primitives
/// provided by the underlying [`DbMechanics`](trait.DbMechanics.html) implementation.
pub trait DbRecordExt<D: DbMechanics>: Sized {
    fn get_current_table_version(db: &D) -> String;
    fn update_current_table_version(db: &D) -> Vec<String>;
    fn table_version(db: &D) -> BTreeMap<String, i32>;
    fn create_table(db: &D) -> Vec<String>;
    fn create_index(db: &D) -> Vec<String>;

    fn offsets_table_name(db: &D) -> String;
    fn create_offsets(db: &D) -> String;
    fn select_offsets(db: &D) -> String;
    fn insert_offset(db: &D) -> String;

    fn insert_record(db: &D) -> Vec<(&'static str, String)>;
    fn delete_record(db: &D) -> Vec<(&'static str, String)>;
    fn values(&self) -> (&'static str, Vec<D::SqlValue>);
}

/// A union of up to five record types that are computed from the same input events
///
/// For one union there is exactly one offset map stored in the database. When using fewer than
/// five variants, the remaining ones will be skipped during table creation and data insertion.
/// It is important that [`DbRecordExt`](trait.DbRecordExt.html) is only implemented for this
/// type and not for single record types.
#[derive(Clone, Debug, Serialize, Deserialize, Abomonation, PartialOrd, Ord, PartialEq, Eq)]
pub enum Union<T1, T2 = (), T3 = (), T4 = (), T5 = ()> {
    T1(T1),
    T2(T2),
    T3(T3),
    T4(T4),
    T5(T5),
}

impl<T> From<T> for Union<T> {
    fn from(t: T) -> Self {
        Union::T1(t)
    }
}

macro_rules! Tvec {
    (T1::$f:ident$arg:tt) => {
        vec![
            T1::$f$arg,
            T2::$f$arg,
            T3::$f$arg,
            T4::$f$arg,
            T5::$f$arg,
        ]
    };
    (($(T1::$f:ident$arg:tt),*)) => {
        vec![
            ($(T1::$f$arg),*),
            ($(T2::$f$arg),*),
            ($(T3::$f$arg),*),
            ($(T4::$f$arg),*),
            ($(T5::$f$arg),*),
        ]
    }
}

impl<D, T1, T2, T3, T4, T5> DbRecordExt<D> for Union<T1, T2, T3, T4, T5>
where
    D: DbMechanics,
    T1: DbRecord<D> + 'static,
    T2: DbRecord<D> + 'static,
    T3: DbRecord<D> + 'static,
    T4: DbRecord<D> + 'static,
    T5: DbRecord<D> + 'static,
{
    fn get_current_table_version(db: &D) -> String {
        let names = Tvec!(T1::table_name())
            .into_iter()
            .filter(|n| !n.is_empty())
            .map(|n| format!(r"'{}'", db.table_name(n)))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "select table_name, version from {} where table_name in ({})",
            SCHEMA_VERSIONS_TABLE_NAME, names
        )
    }

    fn update_current_table_version(db: &D) -> Vec<String> {
        Tvec![(T1::table_name(), T1::table_version())]
            .into_iter()
            .filter(|x| !x.0.is_empty())
            .map(|(name, version)| (db.table_name(name), version))
            .flat_map(|(name, version)| {
                vec![
                    format!(
                        "delete from {} where table_name = '{}'",
                        SCHEMA_VERSIONS_TABLE_NAME, name
                    ),
                    format!(
                        "insert into {} values ('{}', {})",
                        SCHEMA_VERSIONS_TABLE_NAME, name, version
                    ),
                    format!("drop table if exists {}", name),
                    format!("drop table if exists {}_{}", name, OFFSETS_TABLE_SUFFIX),
                ]
            })
            .collect()
    }

    fn table_version(db: &D) -> BTreeMap<String, i32> {
        Tvec![(T1::table_name(), T1::table_version())]
            .into_iter()
            .filter(|x| !x.0.is_empty())
            .map(|x| (db.table_name(x.0), x.1))
            .collect()
    }

    fn create_table(db: &D) -> Vec<String> {
        Tvec![(T1::table_name(), T1::columns())]
            .into_iter()
            .filter(|x| !x.0.is_empty())
            .map(|(name, columns)| {
                let columns = columns
                    .iter()
                    .map(|col| format!("{} {}", col.name, col.tpe))
                    .collect::<Vec<_>>()
                    .join(", ");
                D::create_table(db.table_name(name), columns)
            })
            .collect()
    }

    fn create_index(db: &D) -> Vec<String> {
        Tvec![(T1::table_name(), T1::columns())]
            .into_iter()
            .filter(|x| !x.0.is_empty())
            .flat_map(|(name, columns)| {
                let index = columns
                    .iter()
                    .filter(|col| col.index)
                    .map(|col| col.name)
                    .collect::<Vec<_>>()
                    .join(", ");
                if index.is_empty() {
                    vec![]
                } else {
                    let name = db.table_name(name);
                    vec![D::create_index(format!("{}_index", name), name, index)]
                }
            })
            .collect()
    }

    fn offsets_table_name(db: &D) -> String {
        format!(
            "{}_{}",
            db.table_name(T1::table_name()),
            OFFSETS_TABLE_SUFFIX
        )
    }

    fn create_offsets(db: &D) -> String {
        D::create_table(
            Self::offsets_table_name(db),
            "source varchar(50) primary key, offset_ bigint".to_owned(),
        )
    }

    fn select_offsets(db: &D) -> String {
        format!(
            "select source, offset_ as offset from {}_{}",
            db.table_name(T1::table_name()),
            OFFSETS_TABLE_SUFFIX
        )
    }

    fn insert_offset(db: &D) -> String {
        let mut fields = D::field_iter();
        format!(
            "insert into {}_{} (source, offset_) values ({}, {}) \
            on conflict(source) do update set offset_ = excluded.offset_",
            db.table_name(T1::table_name()),
            OFFSETS_TABLE_SUFFIX,
            fields.next().unwrap(),
            fields.next().unwrap()
        )
    }

    fn insert_record(db: &D) -> Vec<(&'static str, String)> {
        Tvec![(T1::table_name(), T1::columns())]
            .into_iter()
            .filter(|x| !x.0.is_empty())
            .map(|(name, columns)| {
                let fields = D::field_iter();
                let columns = columns
                    .iter()
                    .filter(|col| !col.exclude)
                    .map(|col| col.name)
                    .collect::<Vec<_>>();
                let holes = fields.take(columns.len()).collect::<Vec<_>>().join(", ");
                let columns = columns.join(", ");
                (
                    name,
                    format!(
                        "insert into {} ({}) values ({})",
                        db.table_name(name),
                        columns,
                        holes
                    ),
                )
            })
            .collect()
    }

    fn delete_record(db: &D) -> Vec<(&'static str, String)> {
        Tvec![(T1::table_name(), T1::columns())]
            .into_iter()
            .filter(|x| !x.0.is_empty())
            .map(|(name, columns)| {
                let mut fields = D::field_iter();
                let columns = columns
                    .iter()
                    .filter(|col| !col.exclude)
                    .map(|col| col.name)
                    .collect::<Vec<_>>();
                let matches = columns
                    .iter()
                    .map(|name| D::compare_value(name, fields.next().unwrap()))
                    .collect::<Vec<_>>()
                    .join(" and ");
                (
                    name,
                    D::delete_limit(
                        format!("from {} where {}", db.table_name(name), matches),
                        fields,
                    ),
                )
            })
            .collect()
    }

    fn values(&self) -> (&'static str, Vec<D::SqlValue>) {
        match self {
            Union::T1(t1) => (T1::table_name(), t1.values()),
            Union::T2(t2) => (T2::table_name(), t2.values()),
            Union::T3(t3) => (T3::table_name(), t3.values()),
            Union::T4(t4) => (T4::table_name(), t4.values()),
            Union::T5(t5) => (T5::table_name(), t5.values()),
        }
    }
}

/// needed for the Union default type parameters — unused tables don’t have names and contain nothing
impl<D: DbMechanics> DbRecord<D> for () {
    fn table_version() -> i32 {
        0
    }
    fn table_name() -> &'static str {
        ""
    }
    fn columns() -> &'static [DbColumn] {
        static X: &[DbColumn] = &[];
        X
    }
    fn values(&self) -> Vec<D::SqlValue> {
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mssql::MssqlDbMechanics;
    use postgre::PostgresDbMechanics;
    use sqlite::SqliteDbMechanics;

    #[derive(Clone, Debug)]
    pub struct TestRecord {
        a: &'static str,
        b: i64,
    }

    impl DbRecord<SqliteDbMechanics> for TestRecord {
        fn table_version() -> i32 {
            1
        }
        fn table_name() -> &'static str {
            "test_record"
        }
        fn columns() -> &'static [DbColumn] {
            static X: &[DbColumn] = &[
                DbColumn {
                    name: "a",
                    tpe: "text",
                    exclude: false,
                    index: false,
                },
                DbColumn {
                    name: "b",
                    tpe: "integer",
                    exclude: false,
                    index: false,
                },
            ];
            X
        }
        fn values(&self) -> Vec<<SqliteDbMechanics as DbMechanics>::SqlValue> {
            vec![Box::new(self.a), Box::new(self.b)]
        }
    }

    impl DbRecord<MssqlDbMechanics> for TestRecord {
        fn table_version() -> i32 {
            1
        }
        fn table_name() -> &'static str {
            "test_record"
        }
        fn columns() -> &'static [DbColumn] {
            static X: &[DbColumn] = &[
                DbColumn {
                    name: "a",
                    tpe: "varchar(100)",
                    exclude: false,
                    index: false,
                },
                DbColumn {
                    name: "b",
                    tpe: "bigint",
                    exclude: false,
                    index: false,
                },
            ];
            X
        }
        fn values(&self) -> Vec<<MssqlDbMechanics as DbMechanics>::SqlValue> {
            vec![Box::new(self.a), Box::new(self.b)]
        }
    }

    impl DbRecord<PostgresDbMechanics> for TestRecord {
        fn table_version() -> i32 {
            1
        }
        fn table_name() -> &'static str {
            "test_record"
        }
        fn columns() -> &'static [DbColumn] {
            static X: &[DbColumn] = &[
                DbColumn {
                    name: "a",
                    tpe: "varchar(100)",
                    exclude: false,
                    index: false,
                },
                DbColumn {
                    name: "b",
                    tpe: "bigint",
                    exclude: false,
                    index: false,
                },
            ];
            X
        }
        fn values(&self) -> Vec<<PostgresDbMechanics as DbMechanics>::SqlValue> {
            vec![Box::new(self.a), Box::new(self.b)]
        }
    }

    pub fn test_records() -> Vec<(Union<TestRecord>, isize)> {
        vec![
            (TestRecord { a: "aa", b: 12 }.into(), 1),
            (TestRecord { a: "bb", b: 14 }.into(), 1),
            (TestRecord { a: "cc", b: 22 }.into(), 1),
            (TestRecord { a: "dd", b: 11 }.into(), 1),
        ]
    }

    #[test]
    fn must_unionize() {
        let x: Vec<Union<&str, i32>> = vec![Union::T1("hello"), Union::T2(42)];
        assert_eq!(&*format!("{:?}", x), "[T1(\"hello\"), T2(42)]");
    }
}
