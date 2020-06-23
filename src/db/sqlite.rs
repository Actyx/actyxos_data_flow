/*
 * Copyright 2020 Actyx AG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use super::{DbMechanics, DbRecordExt, DB, SCHEMA_VERSIONS_TABLE_NAME};
use actyxos_sdk::event::{Offset, OffsetMap, SourceId};
use anyhow::Result;
use rusqlite::{params, Connection, OpenFlags, Statement, ToSql, NO_PARAMS};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::{cell::RefCell, fmt::Debug, iter::once, marker::PhantomData, str::FromStr};
use tracing::{debug, field, info, instrument, trace_span};

#[cfg(test)]
use rusqlite::types::Value;

/// Database driver for Sqlite3, based on the rusqlite crate
pub struct SqliteDB<T> {
    conn: RefCell<Connection>,
    mechanics: SqliteDbMechanics,
    ph: PhantomData<T>,
}

impl<T: DbRecordExt<SqliteDbMechanics> + Debug + 'static> SqliteDB<T> {
    pub fn new(prefix: impl Into<String>, db_name: &str) -> Result<Self> {
        let schema_versions_table_schema = "table_name text, version int not null".to_owned();
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_FULL_MUTEX;

        let conn = trace_span!("connect").in_scope(|| -> Result<_> {
            let conn = Connection::open_with_flags(PathBuf::from(db_name), flags)?;

            // `PRAGMA journal_mode = WAL;` https://www.sqlite.org/wal.html
            // This PRAGMA statement returns the new journal mode, so we need to see if it succeeded
            conn.query_row("PRAGMA journal_mode = WAL;", NO_PARAMS, |row| {
                match row.get_raw(0).as_str().unwrap() {
                    "wal" | "memory" => Ok(()),
                    _ => Err(rusqlite::Error::InvalidQuery),
                }
            })?;
            // `PRAGMA synchronous = NORMAL;` https://www.sqlite.org/pragma.html#pragma_synchronous
            conn.execute("PRAGMA synchronous = NORMAL;", NO_PARAMS)?;

            debug!("new connection");
            Ok(conn)
        })?;

        conn.execute_batch(
            format!(
                "{};",
                <Self as DB>::Mechanics::create_table(
                    SCHEMA_VERSIONS_TABLE_NAME.to_string(),
                    schema_versions_table_schema,
                )
            )
            .as_str(),
        )?;

        let mut prefix = prefix.into();
        if !prefix.is_empty() {
            prefix.push('_');
        }
        let ret = Self {
            conn: conn.into(),
            mechanics: SqliteDbMechanics { prefix },
            ph: PhantomData,
        };
        let mut borrow = ret.conn.borrow_mut();
        let tx = borrow.transaction()?;

        let res = tx
            .prepare(T::get_current_table_version(&ret.mechanics).as_str())?
            .query(NO_PARAMS)?
            .mapped(|row| Ok((row.get::<_, String>(0)?, row.get::<_, i32>(1)?)))
            .try_fold(
                BTreeMap::new(),
                |mut map, row| -> Result<BTreeMap<String, i32>, rusqlite::Error> {
                    let (name, version) = row?;
                    map.insert(name, version);
                    Ok(map)
                },
            )
            .unwrap_or_default();

        info!(
            "Sqlite, what we got regarding current_table_version: {:?}",
            &res
        );

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
                tx.execute(&query, NO_PARAMS)?;
            }
        }

        tx.commit()?;
        info!("initialization complete");
        drop(borrow);

        Ok(ret)
    }

    #[cfg(test)]
    #[instrument(skip(self), level = "trace")]
    fn get_records(&mut self) -> Result<Vec<Vec<Value>>> {
        let mut borrow = self.conn.borrow_mut();
        let tx = borrow.transaction()?;
        let mut select = tx.prepare("select * from test_record")?;
        let mut ret = vec![];
        let rows = select.query(NO_PARAMS)?.mapped(|row| {
            let len = row.column_count();
            Ok((0..len)
                .map(move |idx| row.get_raw(idx).into())
                .collect::<Vec<Value>>())
        });
        for row in rows {
            ret.push(row?);
        }
        Ok(ret)
    }
}

/// Database mechanics definitions for the Sqlite3 driver
pub struct SqliteDbMechanics {
    prefix: String,
}

impl DbMechanics for SqliteDbMechanics {
    type SqlValue = Box<dyn ToSql>;

    fn table_prefix(&self) -> &str {
        &self.prefix
    }
    fn field_iter() -> Box<dyn Iterator<Item = String>> {
        Box::new(std::iter::repeat("?".to_owned()))
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
        format!("{} is {}", column, value)
    }
}

impl<T: DbRecordExt<SqliteDbMechanics> + Debug + 'static> DB for SqliteDB<T> {
    type Mechanics = SqliteDbMechanics;
    type Record = T;

    fn name() -> &'static str {
        "Sqlite3(rusqlite)"
    }
    fn get_mechanics(&self) -> &Self::Mechanics {
        &self.mechanics
    }

    #[instrument(skip(self), level = "trace")]
    fn get_offsets(&mut self) -> Result<OffsetMap> {
        let mut borrow = self.conn.borrow_mut();
        let tx = borrow.transaction()?;
        let result = tx
            .prepare(T::select_offsets(&self.mechanics).as_str())?
            .query(NO_PARAMS)?
            .mapped(|row| {
                let source: String = row.get(0)?;
                let offset: i64 = row.get(1)?;
                Ok((SourceId::from_str(source.as_ref()), Offset(offset)))
            })
            .try_fold(HashMap::new(), |mut m, res| -> Result<_> {
                let (s, o) = res?;
                m.insert(s?, o);
                Ok(m)
            })?
            .into();
        tx.commit()?;
        Ok(result)
    }

    #[instrument(skip(self, offsets, deltas), level = "trace")]
    fn advance_offsets<C>(&mut self, offsets: &OffsetMap, deltas: C) -> Result<()>
    where
        C: IntoIterator<Item = (T, isize)>,
    {
        let mut borrow = self.conn.borrow_mut();
        let tx = borrow.transaction()?;

        trace_span!("writing offsets").in_scope(|| -> Result<_> {
            let mut offset = tx.prepare(T::insert_offset(&self.mechanics).as_str())?;
            for (s, o) in offsets.as_ref() {
                offset.execute(params![s.as_str(), o.0])?;
            }
            Ok(())
        })?;

        let mut insert: BTreeMap<&'static str, Statement> = T::insert_record(&self.mechanics)
            .into_iter()
            .map(|stmt| (stmt.0, tx.prepare(&stmt.1).unwrap()))
            .collect();
        let mut delete: BTreeMap<&'static str, Statement> = T::delete_record(&self.mechanics)
            .into_iter()
            .map(|stmt| (stmt.0, tx.prepare(&stmt.1).unwrap()))
            .collect();

        let mut d = 0;
        let span = trace_span!("writing records", deltas = field::Empty);
        let guard = span.enter();
        for (record, mult) in deltas {
            d += 1;
            match mult.cmp(&0) {
                Ordering::Greater => {
                    for _ in 0..mult {
                        let (table, values) = record.values();
                        insert.get_mut(table).unwrap().execute(values)?;
                    }
                }
                Ordering::Less => {
                    let (table, values) = record.values();
                    // sqlite does not support DELETE ... LIMIT without a feature flag that rusqlite does not set
                    let rows = delete.get_mut(table).unwrap().execute(values)?;
                    let to_insert = rows as isize + mult;
                    for _ in 0..to_insert {
                        let (table, values) = record.values();
                        insert.get_mut(table).unwrap().execute(values)?;
                    }
                }
                Ordering::Equal => panic!("cannot insert with multiplicity {}", mult),
            };
        }
        span.record("deltas", &d);
        drop(guard);
        drop(span);
        drop(insert);
        drop(delete);

        tx.commit()?;
        debug!(deltas = d, events = offsets.size(), "done writing");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{tests::*, Union};

    fn s(s: &str) -> Value {
        s.to_owned().into()
    }
    fn n(n: i64) -> Value {
        n.into()
    }

    #[test]
    fn sqlite_must_store() {
        let mut db = SqliteDB::<Union<TestRecord>>::new("".to_owned(), ":memory:").unwrap();
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

        let records = db.get_records().unwrap();
        let expected = vec![
            vec![s("aa"), n(12)],
            vec![s("aa"), n(12)],
            vec![s("bb"), n(14)],
            vec![s("cc"), n(22)],
            vec![s("dd"), n(11)],
        ];
        assert_eq!(records, expected);

        let inputs = test_records()
            .iter()
            .map(|(s, o)| ((*s).clone(), -o))
            .collect::<Vec<_>>();
        db.advance_offsets(&offsets, inputs).unwrap();

        let records = db.get_records().unwrap();
        let expected = vec![vec![s("aa"), n(12)]];
        assert_eq!(records, expected);
    }
}
