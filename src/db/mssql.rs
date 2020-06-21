use super::{DbMechanics, DbRecordExt, DB, SCHEMA_VERSIONS_TABLE_NAME};
use actyxos_sdk::event::{Offset, OffsetMap, SourceId};
use anyhow::Result;
use derive_more::From;
use futures01::future::Future as Future01;
use futures_state_stream::StateStream;
use std::cmp::Ordering;
use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    fmt::Display,
    iter::once,
    marker::PhantomData,
    str::FromStr,
};
use tiberius::{stmt::Statement, ty::ToSql, BoxableIo, SqlConnection};
use tracing::{debug, field, info, instrument, trace_span};

type Conn = SqlConnection<Box<dyn BoxableIo>>;

/// Database driver for Microsoft SQL Server, based on the Tiberius crate
pub struct MssqlDB<T> {
    conn_str: String,
    conn: Option<Conn>,
    mechanics: MssqlDbMechanics,
    ph: PhantomData<T>,
}

// Tiberius::Error is really unfriendly, so we must massage it ...
#[derive(Debug, From)]
pub struct TiberiusError(tiberius::Error);
impl Display for TiberiusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}
impl Error for TiberiusError {}

trait TE {
    type Output;
    fn te(self) -> Self::Output;
}
impl<T> TE for std::result::Result<T, tiberius::Error> {
    type Output = std::result::Result<T, TiberiusError>;
    fn te(self) -> Self::Output {
        self.map_err(TiberiusError::from)
    }
}

fn with_sql<T>(v: Vec<Box<dyn ToSql>>, f: impl FnOnce(&[&dyn ToSql]) -> Result<T>) -> Result<T> {
    let v2 = v.iter().map(|b| &**b).collect::<Vec<_>>();
    f(v2.as_slice())
}

impl<T: DbRecordExt<MssqlDbMechanics> + 'static> MssqlDB<T> {
    pub fn new(prefix: impl Into<String>, conn_str: impl Into<String>) -> Result<Self> {
        let schema_versions_table_schema =
            "table_name varchar(max), version int not null".to_owned();
        let mut prefix = prefix.into();
        if !prefix.is_empty() {
            prefix.push('_');
        }
        let mut ret = Self {
            conn_str: conn_str.into(),
            conn: None,
            mechanics: MssqlDbMechanics { prefix },
            ph: PhantomData,
        };
        let conn = ret.connect().te()?;
        let tx = conn.transaction().wait().te()?;

        // DO NOT USE simple_exec, it just panics upon receiving the server response
        let (_, tx) = tx
            .exec(
                <Self as DB>::Mechanics::create_table(
                    SCHEMA_VERSIONS_TABLE_NAME.to_string(),
                    schema_versions_table_schema,
                ),
                &[],
            )
            .wait()
            .unwrap();

        let (res, tx) = tx
            .query(T::get_current_table_version(&ret.mechanics), &[])
            .map(|row| (row.get::<_, &str>(0).to_owned(), row.get::<_, i32>(1)))
            .collect()
            .wait()
            .unwrap();
        let res: BTreeMap<String, i32> = res.into_iter().collect();

        info!(
            "MSSQL, what we got regarding current_table_version: {:?}",
            &res
        );

        let mut tx = tx;
        let expected_versions = T::table_version(&ret.mechanics);
        if res != expected_versions {
            info!(
                "migrating schema from version {:?} to version {:?}",
                res, expected_versions
            );
            for query in T::update_current_table_version(&ret.mechanics)
                .into_iter()
                .chain(once(T::create_offsets(&ret.mechanics)))
                .chain(T::create_table(&ret.mechanics))
                .chain(T::create_index(&ret.mechanics))
            {
                tx = tx.exec(query, &[]).wait().te()?.1;
            }
        }

        let conn = tx.commit().wait().te()?;
        info!("initialization complete");
        ret.conn = Some(conn);
        Ok(ret)
    }

    #[instrument(skip(self), level = "debug")]
    fn connect(&mut self) -> Result<Conn, tiberius::Error> {
        match self.conn.take() {
            Some(conn) => Ok(conn),
            None => {
                let conn = SqlConnection::connect(&*self.conn_str).wait()?;
                let (_, conn) = conn
                    .exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
                    .wait()?;
                debug!("new connection");
                Ok(conn)
            }
        }
    }

    #[cfg(test)]
    #[instrument(skip(self), level = "debug")]
    fn get_records<X: tests::ParseRow>(&mut self) -> Result<Vec<X>> {
        let conn = self.connect().te()?;
        let tx = conn.transaction().wait().te()?;

        let (rows, tx) = tx
            .query("select * from test_record", &[])
            .map(X::parse_row)
            .collect()
            .wait()
            .te()?;

        let conn = tx.commit().wait().te()?;
        self.conn = Some(conn);
        Ok(rows)
    }

    #[cfg(test)]
    #[instrument(skip(self), level = "debug")]
    fn clear_database(&mut self) -> Result<()> {
        let conn = self.connect().te()?;

        let (_, conn) = conn.exec("delete from test_record", &[]).wait().te()?;
        let (_, conn) = conn
            .exec("delete from test_record_offsets", &[])
            .wait()
            .te()?;

        self.conn = Some(conn);
        Ok(())
    }
}

/// Database mechanics definitions for the Microsoft SQL Server driver
pub struct MssqlDbMechanics {
    prefix: String,
}

impl DbMechanics for MssqlDbMechanics {
    type SqlValue = Box<dyn ToSql>;

    fn table_prefix(&self) -> &str {
        &self.prefix
    }

    fn field_iter() -> Box<dyn Iterator<Item = String>> {
        Box::new((1..usize::MAX).map(|i| format!("@P{}", i)))
    }
    fn create_table(name: String, definition: String) -> String {
        format!(
            "if not exists (select * from sys.tables where name = '{name}') \
                create table {name} ({definition})",
            name = name,
            definition = definition
        )
    }
    fn create_index(name: String, table: String, definition: String) -> String {
        format!(
            "if not exists (select * from sys.indexes where name = '{name}') \
                create index {name} on {table} ({definition})",
            name = name,
            table = table,
            definition = definition
        )
    }
    fn delete_limit(clause: String, mut fields: Box<dyn Iterator<Item = String>>) -> String {
        format!("delete top ({}) {}", fields.next().unwrap(), clause)
    }
    fn compare_value(column: &str, value: String) -> String {
        format!(
            "( {column} = {value} or \
               (case when {column} is null and {value} is null then 1 else 0 end) = 1 )",
            column = column,
            value = value
        )
    }
}

impl<T: DbRecordExt<MssqlDbMechanics> + 'static> DB for MssqlDB<T> {
    type Mechanics = MssqlDbMechanics;
    type Record = T;

    #[instrument(skip(self), level = "trace")]
    fn get_offsets(&mut self) -> Result<OffsetMap> {
        let conn = self.connect().te()?;

        let tx = trace_span!("opening transaction").in_scope(|| conn.transaction().wait().te())?;

        let (vec, tx) = trace_span!("getting offsets").in_scope(|| {
            tx.query(T::select_offsets(&self.mechanics), &[])
                .map(|row| {
                    let source: &str = row.get(0);
                    let offset: i64 = row.get(1);
                    (SourceId::from_str(source).unwrap(), Offset(offset))
                })
                .collect()
                .wait()
                .te()
        })?;

        let map = vec.into_iter().collect::<HashMap<_, _>>();
        let offsets = OffsetMap::from(map);

        self.conn = Some(tx.commit().wait().te()?);
        Ok(offsets)
    }

    #[instrument(skip(self, offsets, deltas), level = "trace")]
    fn advance_offsets<C>(&mut self, offsets: &OffsetMap, deltas: C) -> Result<()>
    where
        C: IntoIterator<Item = (T, isize)>,
    {
        let conn = self.connect().te()?;

        let (mut tx, offsets_table_name) =
            trace_span!("opening transaction").in_scope(|| -> Result<_> {
                let tx = conn.transaction().wait().te()?;

                // In the evil "implicit_transactions on" mode that tiberius uses, certain queries
                // implicitly start a transaction. SELECT is one of them, IF, however is not ;)
                // https://docs.microsoft.com/en-us/sql/t-sql/statements/set-implicit-transactions-transact-sql?view=sql-server-ver15
                // Because tiberius does COMMIT but does not explicitly BEGIN TRANSACTION, if you do not start with any of those queries,
                // you'll get a cryptic error - https://stackoverflow.com/questions/5623840/the-commit-transaction-request-has-no-corresponding-begin-transaction
                // You can always check if you are in a transaction by SELECT @@TranCount; (should return > 0).
                let offsets_table_name = T::offsets_table_name(&self.mechanics);
                let select_query = format!("select top 1 * from {};", offsets_table_name);
                let (_, tx) = tx.query(select_query, &[]).collect().wait().te()?;
                Ok((tx, offsets_table_name))
            })?;

        let mut fields = MssqlDbMechanics::field_iter();
        let query = format!(
            // WARNING: this only works when this is the only writing process on that table!
            // see https://michaeljswart.com/2017/07/sql-server-upsert-patterns-and-antipatterns/
            "if exists (select * from {table} with (updlock) where source = {source}) \
                update {table} set offset_ = {offset} where source = {source} \
             else \
                insert {table} (source, offset_) values ({source}, {offset})",
            table = offsets_table_name,
            source = fields.next().unwrap(),
            offset = fields.next().unwrap(),
        );
        let offset = tx.prepare(query);

        let mut tx = trace_span!("writing offsets").in_scope(|| -> Result<_> {
            for (s, o) in offsets.as_ref() {
                let (_, t) = tx
                    .exec(offset.clone(), &[&s.as_str() as &dyn ToSql, &o.0])
                    .wait()
                    .te()?;
                tx = t;
            }
            Ok(tx)
        })?;

        let insert: BTreeMap<&'static str, Statement> = T::insert_record(&self.mechanics)
            .into_iter()
            .map(|stmt| (stmt.0, tx.prepare(stmt.1)))
            .collect();
        let delete: BTreeMap<&'static str, Statement> = T::delete_record(&self.mechanics)
            .into_iter()
            .map(|stmt| (stmt.0, tx.prepare(stmt.1)))
            .collect();

        let mut d = 0;
        let span = trace_span!("writing records", deltas = field::Empty);
        let guard = span.enter();
        for (record, mult) in deltas {
            d += 1;
            let (table, values) = record.values();
            match mult.cmp(&0) {
                Ordering::Greater => {
                    tx = with_sql(values, |params| {
                        let mut tx = tx;
                        for _ in 0..mult {
                            let (_, t) = tx.exec(insert[table].clone(), params).wait().te()?;
                            tx = t;
                        }
                        Ok(tx)
                    })?;
                }
                Ordering::Less => {
                    let mut v = values;
                    v.push(Box::new(-mult as i64));
                    tx = with_sql(v, |params| {
                        let (_, tx) = tx.exec(delete[table].clone(), params).wait().te()?;
                        Ok(tx)
                    })?;
                }
                Ordering::Equal => panic!("cannot insert with multiplicity {}", mult),
            }
        }
        span.record("deltas", &d);
        drop(guard);
        drop(span);

        let conn = tx.commit().wait().te()?;
        self.conn = Some(conn);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{tests::*, Union};
    use tiberius::query::QueryRow;

    pub trait ParseRow {
        fn parse_row(row: QueryRow) -> Self;
    }

    impl<X: ParseColumn, Y: ParseColumn> ParseRow for (X, Y) {
        fn parse_row(row: QueryRow) -> Self {
            (X::parse_column(&row, 0), Y::parse_column(&row, 1))
        }
    }

    pub trait ParseColumn {
        fn parse_column(row: &QueryRow, idx: usize) -> Self;
    }
    impl ParseColumn for i64 {
        fn parse_column(row: &QueryRow, idx: usize) -> Self {
            row.get(idx)
        }
    }
    impl ParseColumn for String {
        fn parse_column(row: &QueryRow, idx: usize) -> Self {
            let s: &str = row.get(idx);
            s.to_owned()
        }
    }

    fn s(s: &str) -> String {
        s.to_owned()
    }

    #[test_env_log::test]
    fn mssql_must_store() {
        let mut db = if let Ok(conn_str) = std::env::var("MSSQL_CONNECT") {
            MssqlDB::<Union<TestRecord>>::new("".to_owned(), conn_str).unwrap()
        } else {
            return;
        };
        db.clear_database().unwrap();

        let offsets = db.get_offsets().unwrap();
        assert_eq!(offsets, OffsetMap::empty());

        let mut offsets = offsets.into_inner();
        offsets.insert(SourceId::from_str("abc").unwrap(), Offset(42));
        offsets.insert(SourceId::from_str("def").unwrap(), Offset(22));
        let offsets = OffsetMap::from(offsets);
        let mut inputs = test_records();
        inputs[0].1 = 2;
        db.advance_offsets(&offsets, inputs).unwrap();

        let offsets2 = db.get_offsets().unwrap();
        assert_eq!(offsets2, offsets);

        let records: Vec<(String, i64)> = db.get_records().unwrap();
        let expected = vec![
            (s("aa"), 12),
            (s("aa"), 12),
            (s("bb"), 14),
            (s("cc"), 22),
            (s("dd"), 11),
        ];
        assert_eq!(records, expected);

        let inputs = test_records()
            .iter()
            .map(|(s, o)| ((*s).clone(), -o))
            .collect::<Vec<_>>();
        db.advance_offsets(&offsets, inputs).unwrap();

        let records: Vec<(String, i64)> = db.get_records().unwrap();
        let expected = vec![(s("aa"), 12)];
        assert_eq!(records, expected);
    }
}
