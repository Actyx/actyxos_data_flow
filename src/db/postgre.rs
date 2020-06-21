use super::{DbMechanics, DbRecordExt, DB, SCHEMA_VERSIONS_TABLE_NAME};
use actyxos_sdk::event::{Offset, OffsetMap, SourceId};
use anyhow::Result;
use native_tls::TlsConnector;
use postgres::{fallible_iterator::FallibleIterator, types::ToSql, Client, Statement};
use postgres_native_tls::MakeTlsConnector;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::{
    cell::RefCell,
    iter::{empty, once},
    marker::PhantomData,
    str::FromStr,
};
use tracing::info;

/// Database driver for PostgreSQL, based on the postgres crate
pub struct PostgresDB<T> {
    conn: RefCell<Client>,
    mechanics: PostgresDbMechanics,
    ph: PhantomData<T>,
}

fn with_sql<T>(
    v: Vec<Box<dyn ToSql + Sync>>,
    f: impl FnOnce(&[&(dyn ToSql + Sync)]) -> Result<T>,
) -> Result<T> {
    let v2 = v.iter().map(|b| &**b).collect::<Vec<_>>();
    f(v2.as_slice())
}

impl<T: DbRecordExt<PostgresDbMechanics> + 'static> PostgresDB<T> {
    pub fn new(prefix: impl Into<String>, db_name: &str) -> Result<Self> {
        let schema_versions_table_schema = "table_name text, version int not null".to_owned();
        let tls_mode = MakeTlsConnector::new(TlsConnector::new()?);
        let conn = Client::connect(db_name, tls_mode)?;
        let mut prefix = prefix.into();
        if !prefix.is_empty() {
            prefix.push('_');
        }
        let ret = Self {
            conn: conn.into(),
            mechanics: PostgresDbMechanics { prefix },
            ph: PhantomData,
        };

        let mut borrow = ret.conn.borrow_mut();
        let mut tx = borrow.transaction()?;
        tx.execute(
            <Self as DB>::Mechanics::create_table(
                SCHEMA_VERSIONS_TABLE_NAME.to_string(),
                schema_versions_table_schema,
            )
            .as_str(),
            &[],
        )?;
        let res: BTreeMap<String, i32> = tx
            .query_raw(
                T::get_current_table_version(&ret.mechanics).as_str(),
                empty(),
            )?
            .map(|row| Ok((row.get::<_, String>(0), row.get::<_, i32>(1))))
            .collect()?;

        info!(
            "Postgres, what we got regarding current_table_version: {:?}",
            &res
        );

        if res != T::table_version(&ret.mechanics) {
            for query in T::update_current_table_version(&ret.mechanics)
                .into_iter()
                .chain(once(T::create_offsets(&ret.mechanics)))
                .chain(T::create_table(&ret.mechanics))
                .chain(T::create_index(&ret.mechanics))
            {
                tx.execute(query.as_str(), &[])?;
            }
        }

        tx.commit()?;
        drop(borrow);

        Ok(ret)
    }

    #[cfg(test)]
    fn get_records<X: tests::ParseRow>(&mut self) -> Result<Vec<X>> {
        let mut borrow = self.conn.borrow_mut();
        let mut tx = borrow.transaction()?;
        let res = tx
            .query_raw("select * from test_record", empty())?
            .map(|row| Ok(X::parse_row(row)))
            .collect()?;
        Ok(res)
    }

    #[cfg(test)]
    fn clear_database(&mut self) -> Result<()> {
        let mut borrow = self.conn.borrow_mut();
        let mut tx = borrow.transaction()?;
        tx.batch_execute("delete from test_record; delete from test_record_offsets")?;
        tx.commit()?;
        Ok(())
    }
}

/// Database mechanics definitions for the PostgreSQL driver
pub struct PostgresDbMechanics {
    prefix: String,
}

impl DbMechanics for PostgresDbMechanics {
    type SqlValue = Box<dyn ToSql + Sync>;

    fn table_prefix(&self) -> &str {
        &self.prefix
    }
    fn field_iter() -> Box<dyn Iterator<Item = String>> {
        Box::new((1..usize::MAX).map(|i| format!("${}", i)))
    }
    fn create_table(name: String, definition: String) -> String {
        format!("create table if not exists {} ({})", name, definition)
    }
    fn create_index(name: String, table: String, definition: String) -> String {
        format!(
            "create index if not exists {} on {} ({})",
            name, table, definition
        )
    }
    fn delete_limit(clause: String, _fields: Box<dyn Iterator<Item = String>>) -> String {
        format!("delete {}", clause)
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

impl<T: DbRecordExt<PostgresDbMechanics> + 'static> DB for PostgresDB<T> {
    type Mechanics = PostgresDbMechanics;
    type Record = T;

    fn get_offsets(&mut self) -> Result<OffsetMap> {
        let mut borrow = self.conn.borrow_mut();
        let mut tx = borrow.transaction()?;
        let result = tx
            .query_raw(T::select_offsets(&self.mechanics).as_str(), empty())?
            .map(|row| {
                let source: &str = row.try_get(0)?;
                let offset: i64 = row.try_get(1)?;
                Ok((SourceId::from_str(source), Offset(offset)))
            })
            .try_fold(HashMap::new(), |mut m, res| -> Result<_> {
                let (s, o) = res;
                m.insert(s?, o);
                Ok(m)
            })?
            .into();
        tx.commit()?;
        Ok(result)
    }

    fn advance_offsets<C>(&mut self, offsets: &OffsetMap, deltas: C) -> Result<()>
    where
        C: IntoIterator<Item = (T, isize)>,
    {
        let mut borrow = self.conn.borrow_mut();
        let mut tx = borrow.transaction()?;

        let offset = tx.prepare(T::insert_offset(&self.mechanics).as_str())?;
        for (s, o) in offsets.as_ref() {
            tx.execute(&offset, &[&s.as_str(), &o.0])?;
        }

        let insert: BTreeMap<&'static str, Statement> = T::insert_record(&self.mechanics)
            .into_iter()
            .map(|stmt| (stmt.0, tx.prepare(stmt.1.as_str()).unwrap()))
            .collect();
        let delete: BTreeMap<&'static str, Statement> = T::delete_record(&self.mechanics)
            .into_iter()
            .map(|stmt| (stmt.0, tx.prepare(stmt.1.as_str()).unwrap()))
            .collect();

        for (record, mult) in deltas {
            let (table, values) = record.values();
            match mult.cmp(&0) {
                Ordering::Greater => with_sql(values, |params| {
                    for _ in 0..mult {
                        tx.execute(&insert[table], params)?;
                    }
                    Ok(())
                })?,
                Ordering::Less => {
                    with_sql(values, |params| {
                        let removed = tx.execute(&delete[table], params)?;
                        let to_insert = removed as i64 + mult as i64;
                        for _ in 0..to_insert {
                            tx.execute(&insert[table], params)?;
                        }
                        Ok(())
                    })?;
                }
                Ordering::Equal => panic!("cannot insert with multiplicity {}", mult),
            };
        }

        tx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{tests::*, Union};
    use postgres::Row;

    pub trait ParseRow {
        fn parse_row(row: Row) -> Self;
    }

    impl<X: ParseColumn, Y: ParseColumn> ParseRow for (X, Y) {
        fn parse_row(row: Row) -> Self {
            (X::parse_column(&row, 0), Y::parse_column(&row, 1))
        }
    }

    pub trait ParseColumn {
        fn parse_column(row: &Row, idx: usize) -> Self;
    }
    impl ParseColumn for i64 {
        fn parse_column(row: &Row, idx: usize) -> Self {
            row.get(idx)
        }
    }
    impl ParseColumn for String {
        fn parse_column(row: &Row, idx: usize) -> Self {
            let s: &str = row.get(idx);
            s.to_owned()
        }
    }

    fn s(s: &str) -> String {
        s.to_owned()
    }

    #[test]
    fn postgres_must_store() {
        let mut db = if let Ok(conn_str) = std::env::var("POSTGRES_CONNECT") {
            PostgresDB::<Union<TestRecord>>::new("".to_owned(), conn_str.as_str()).unwrap()
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
