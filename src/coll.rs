use std::collections::btree_map::{self, Iter};
use std::collections::BTreeMap;
use std::ops::AddAssign;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::Data;

/// A collection implementation for consolidating and batching deltas
///
/// Insert incoming pairs of data and multiplicity using the `+=` operator.
/// Turn into an iterator or a vector to further consume the batch.
#[derive(Debug, Default)]
pub struct Coll<D: Data, R: Semigroup>(BTreeMap<D, R>);

impl<D: Data, R: Semigroup> Coll<D, R> {
    pub fn new() -> Coll<D, R> {
        Coll(Default::default())
    }

    pub fn from_map(map: BTreeMap<D, R>) -> Coll<D, R> {
        Coll(map)
    }

    pub fn to_vec<Out: From<D>>(&self) -> Vec<(Out, R)> {
        self.0
            .iter()
            .map(|(d, r)| (Out::from(d.clone()), r.clone()))
            .collect()
    }

    pub fn iter(&self) -> Iter<D, R> {
        self.0.iter()
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl<D: Data, R: Semigroup> IntoIterator for Coll<D, R> {
    type Item = (D, R);
    type IntoIter = btree_map::IntoIter<D, R>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, D: Data, R: Semigroup> IntoIterator for &'a Coll<D, R> {
    type Item = (&'a D, &'a R);
    type IntoIter = btree_map::Iter<'a, D, R>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Coll<usize, i64> {
    pub fn new_time() -> Coll<usize, i64> {
        let mut coll = Self::new();
        coll += (0, 1);
        coll
    }
}

impl<D: Data, R: Semigroup> AddAssign<(D, R)> for Coll<D, R> {
    fn add_assign(&mut self, other: (D, R)) {
        let (key, value) = other;
        let v = self
            .0
            .entry(key.clone())
            .and_modify(|x| *x += &value)
            .or_insert(value);
        if v.is_zero() {
            self.0.remove(&key);
        }
    }
}
