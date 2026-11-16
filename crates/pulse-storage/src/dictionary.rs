use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::error::{Result, StorageError};

pub const MAX_DICTIONARY_ENTRIES: usize = (u16::MAX as usize) + 1;

/// Serializable snapshot of a project's `event_id` (`u16`) and `prop_key_id` (`u16`) dictionaries.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectDictSnapshot {
    pub events: Vec<String>,
    pub prop_keys: Vec<String>,
}

/// Thread-safe per-project dictionary mapping event names and property keys to dense `u16` IDs.
#[derive(Debug, Default)]
pub struct ProjectDictionary {
    event_to_id: DashMap<String, u16>,
    events: RwLock<Vec<String>>,

    prop_to_id: DashMap<String, u16>,
    prop_keys: RwLock<Vec<String>>,
}

impl ProjectDictionary {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_snapshot(snap: ProjectDictSnapshot) -> Self {
        let dict = Self::new();
        {
            let mut ev_guard = dict.events.write();
            for (idx, name) in snap
                .events
                .into_iter()
                .take(MAX_DICTIONARY_ENTRIES)
                .enumerate()
            {
                dict.event_to_id.insert(name.clone(), idx as u16);
                ev_guard.push(name);
            }
        }
        {
            let mut pk_guard = dict.prop_keys.write();
            for (idx, name) in snap
                .prop_keys
                .into_iter()
                .take(MAX_DICTIONARY_ENTRIES)
                .enumerate()
            {
                dict.prop_to_id.insert(name.clone(), idx as u16);
                pk_guard.push(name);
            }
        }
        dict
    }

    /// Fallible event interning returning `Err(StorageError::DictionaryFull)` instead of silently aliasing onto `u16::MAX` (`P1-12`).
    #[inline]
    pub fn try_get_or_intern_event(&self, event_name: &str) -> Result<u16> {
        if let Some(id) = self.event_to_id.get(event_name) {
            return Ok(*id);
        }
        let mut guard = self.events.write();
        if let Some(id) = self.event_to_id.get(event_name) {
            return Ok(*id);
        }
        if guard.len() >= MAX_DICTIONARY_ENTRIES {
            return Err(StorageError::DictionaryFull { kind: "events" });
        }
        let id = guard.len() as u16;
        guard.push(event_name.to_string());
        self.event_to_id.insert(event_name.to_string(), id);
        Ok(id)
    }

    #[inline]
    pub fn get_or_intern_event(&self, event_name: &str) -> u16 {
        self.try_get_or_intern_event(event_name)
            .expect("project event dictionary exceeded 65536 entries")
    }

    #[inline]
    pub fn lookup_event(&self, event_name: &str) -> Option<u16> {
        self.event_to_id.get(event_name).map(|r| *r)
    }

    pub fn event_name(&self, id: u16) -> Option<String> {
        self.events.read().get(id as usize).cloned()
    }

    pub fn all_events(&self) -> Vec<(u16, String)> {
        self.events
            .read()
            .iter()
            .enumerate()
            .map(|(i, s)| (i as u16, s.clone()))
            .collect()
    }

    /// Fallible property-key interning returning `Err(StorageError::DictionaryFull)` instead of silently aliasing onto `u16::MAX` (`P1-12`).
    #[inline]
    pub fn try_get_or_intern_prop_key(&self, key: &str) -> Result<u16> {
        if let Some(id) = self.prop_to_id.get(key) {
            return Ok(*id);
        }
        let mut guard = self.prop_keys.write();
        if let Some(id) = self.prop_to_id.get(key) {
            return Ok(*id);
        }
        if guard.len() >= MAX_DICTIONARY_ENTRIES {
            return Err(StorageError::DictionaryFull { kind: "prop_keys" });
        }
        let id = guard.len() as u16;
        guard.push(key.to_string());
        self.prop_to_id.insert(key.to_string(), id);
        Ok(id)
    }

    #[inline]
    pub fn get_or_intern_prop_key(&self, key: &str) -> u16 {
        self.try_get_or_intern_prop_key(key)
            .expect("project property key dictionary exceeded 65536 entries")
    }

    #[inline]
    pub fn lookup_prop_key(&self, key: &str) -> Option<u16> {
        self.prop_to_id.get(key).map(|r| *r)
    }

    pub fn prop_key_name(&self, id: u16) -> Option<String> {
        self.prop_keys.read().get(id as usize).cloned()
    }

    pub fn all_prop_keys(&self) -> Vec<(u16, String)> {
        self.prop_keys
            .read()
            .iter()
            .enumerate()
            .map(|(i, s)| (i as u16, s.clone()))
            .collect()
    }

    pub fn event_count(&self) -> usize {
        self.events.read().len()
    }

    pub fn prop_key_count(&self) -> usize {
        self.prop_keys.read().len()
    }

    pub fn snapshot(&self) -> ProjectDictSnapshot {
        ProjectDictSnapshot {
            events: self.events.read().clone(),
            prop_keys: self.prop_keys.read().clone(),
        }
    }
}
