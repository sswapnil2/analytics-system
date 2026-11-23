use std::collections::HashMap;

/// Erasure tombstones for one project: raw uid → erasure time (ms).
///
/// An erased id hides only events at or before its erasure time, so a person who signs up again
/// with the same id is counted from then on. Rows erased before tombstones carried a time are
/// stored with `i64::MAX` and stay hidden for good.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ErasedUids {
    at: HashMap<u32, i64>,
}

impl ErasedUids {
    pub fn is_empty(&self) -> bool {
        self.at.is_empty()
    }

    pub fn len(&self) -> usize {
        self.at.len()
    }

    /// Records `uid` as erased at `at_ms`; a later erasure of the same id moves the cut-off forward.
    pub fn insert(&mut self, uid: u32, at_ms: i64) {
        let e = self.at.entry(uid).or_insert(at_ms);
        *e = (*e).max(at_ms);
    }

    /// True when `uid` has an erasure tombstone recorded (regardless of timestamp).
    #[inline]
    pub fn contains(&self, uid: u32) -> bool {
        self.at.contains_key(&uid)
    }

    /// Iterates `(uid, erased_at_ms)` pairs in this set.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (u32, i64)> + '_ {
        self.at.iter().map(|(&u, &t)| (u, t))
    }

    /// True when an event by `uid` at `ts_ms` must not be shown or kept.
    #[inline]
    pub fn hides(&self, uid: u32, ts_ms: i64) -> bool {
        self.at.get(&uid).is_some_and(|&t| ts_ms <= t)
    }
}

impl FromIterator<(u32, i64)> for ErasedUids {
    fn from_iter<I: IntoIterator<Item = (u32, i64)>>(iter: I) -> Self {
        let mut s = Self::default();
        for (u, t) in iter {
            s.insert(u, t);
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hides_only_up_to_the_erasure_time() {
        let mut e = ErasedUids::default();
        e.insert(7, 1_000);
        assert!(e.hides(7, 999));
        assert!(e.hides(7, 1_000));
        assert!(!e.hides(7, 1_001), "a re-signup after erasure is counted");
        assert!(!e.hides(8, 0));
        e.insert(7, 500);
        assert!(
            e.hides(7, 900),
            "an older second erasure never shrinks the cut-off"
        );
    }
}
