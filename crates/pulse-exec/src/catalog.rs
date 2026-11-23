use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};

use arrow_array::builder::{BooleanBuilder, Float64Builder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use arrow_array::{Array as ArrowArray, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use chrono_tz::Tz;
use dashmap::DashMap;
use datafusion::common::DFSchema;
use datafusion::prelude::{SessionConfig, SessionContext};
use parking_lot::RwLock;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pulse_identity::IdentitySnapshot;
use pulse_storage::extract_arrow_str;
use pulse_wire::PropValue;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::ast::{BehavioralCohortDef, PropFilterOp, PropPredicate};

/// Shared DataFusion `SessionContext` with `enable_ident_normalization = false` so mixed-case
/// property names like `userPlan` resolve without being lowercased to `userplan` (`P2-09i`).
fn shared_df_context() -> &'static SessionContext {
    static CTX: OnceLock<SessionContext> = OnceLock::new();
    CTX.get_or_init(|| {
        let mut cfg = SessionConfig::new();
        cfg.options_mut().sql_parser.enable_ident_normalization = false;
        SessionContext::new_with_config(cfg)
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomEventDef {
    pub name: String,
    pub source_events: Vec<String>,
    #[serde(default)]
    pub filter: Option<PropPredicate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomPropertyDef {
    pub name: String,
    /// SQL expression evaluated via DataFusion, e.g. `CASE WHEN userPlan = 'Pro' THEN amount * 1.5 ELSE amount END`.
    pub sql_expr: String,
    /// Explicit base property columns referenced by `sql_expr` (auto-extracted if empty).
    #[serde(default)]
    pub referenced_props: Vec<String>,
}

impl CustomPropertyDef {
    pub fn referenced_columns(&self) -> Vec<String> {
        if !self.referenced_props.is_empty() {
            return self.referenced_props.clone();
        }
        extract_sql_identifiers(&self.sql_expr)
    }
}

/// Extracts candidate column identifiers (both unquoted and `"doubleQuoted"`) from an inline SQL expression, excluding SQL keywords (`P2-09i`).
pub fn extract_sql_identifiers(sql: &str) -> Vec<String> {
    const KEYWORDS: &[&str] = &[
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "AND",
        "OR",
        "NOT",
        "NULL",
        "IS",
        "IN",
        "TRUE",
        "FALSE",
        "AS",
        "CAST",
        "COALESCE",
        "LOWER",
        "UPPER",
        "ABS",
        "ROUND",
        "FLOOR",
        "CEIL",
        "CONCAT",
        "SUBSTR",
        "SUBSTRING",
        "LENGTH",
        "LIKE",
        "ILIKE",
        "BETWEEN",
        "FLOAT",
        "DOUBLE",
        "BIGINT",
        "INT",
        "VARCHAR",
        "TEXT",
    ];
    let kw_set: HashSet<&str> = KEYWORDS.iter().copied().collect();

    let mut out = Vec::new();
    let mut seen = HashSet::new();
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        // Skip single-quoted string literals
        if b == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    i += 1;
                    if i < bytes.len() && bytes[i] == b'\'' {
                        i += 1;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            continue;
        }
        // Double-quoted column identifier: `"userPlan"`
        if b == b'"' {
            let start = i + 1;
            i += 1;
            while i < bytes.len() && bytes[i] != b'"' {
                i += 1;
            }
            let ident = &sql[start..i];
            if i < bytes.len() {
                i += 1;
            }
            if !ident.is_empty() && seen.insert(ident.to_string()) {
                out.push(ident.to_string());
            }
            continue;
        }
        if b.is_ascii_alphabetic() || b == b'_' || b == b'$' {
            let start = i;
            i += 1;
            while i < bytes.len()
                && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'$')
            {
                i += 1;
            }
            let ident = &sql[start..i];
            let next_non_ws = sql[i..].chars().find(|c| !c.is_whitespace());
            if next_non_ws == Some('(') {
                continue;
            }
            let upper = ident.to_ascii_uppercase();
            if !kw_set.contains(upper.as_str()) && seen.insert(ident.to_string()) {
                out.push(ident.to_string());
            }
        } else {
            i += 1;
        }
    }
    out
}

/// Lowercases unquoted SQL function names (e.g. `COALESCE(...)` -> `coalesce(...)`) while preserving
/// exact casing of column identifiers when `enable_ident_normalization = false` (`P2-09i`).
fn normalize_sql_function_calls(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' || b == b'"' {
            let quote = b;
            out.push(quote as char);
            i += 1;
            while i < bytes.len() {
                out.push(bytes[i] as char);
                if bytes[i] == quote {
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if b.is_ascii_alphabetic() || b == b'_' {
            let start = i;
            i += 1;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let token = &sql[start..i];
            let next_non_ws = sql[i..].chars().find(|c| !c.is_whitespace());
            if next_non_ws == Some('(') {
                out.push_str(&token.to_ascii_lowercase());
            } else {
                out.push_str(token);
            }
        } else {
            out.push(sql[i..].chars().next().unwrap());
            i += out.chars().last().unwrap().len_utf8();
        }
    }
    out
}

/// Evaluates a `CustomPropertyDef` SQL expression against coalesced base property columns via DataFusion's `PhysicalExpr`.
pub fn evaluate_custom_property(
    def: &CustomPropertyDef,
    num_rows: usize,
    base_cols: &HashMap<String, Arc<Vec<PropValue>>>,
) -> Result<Vec<PropValue>, String> {
    if num_rows == 0 {
        return Ok(Vec::new());
    }

    let refs = def.referenced_columns();
    let mut fields = Vec::with_capacity(refs.len().max(1));
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(refs.len().max(1));

    if refs.is_empty() {
        let mut b = Float64Builder::with_capacity(num_rows);
        for _ in 0..num_rows {
            b.append_value(1.0);
        }
        fields.push(Field::new("_dummy", DataType::Float64, false));
        arrays.push(Arc::new(b.finish()));
    } else {
        for col_name in &refs {
            let vals = base_cols.get(col_name);
            let inferred_type = infer_best_arrow_type(vals.map(|v| v.as_slice()));
            match inferred_type {
                DataType::Float64 => {
                    let mut fb = Float64Builder::with_capacity(num_rows);
                    for r in 0..num_rows {
                        if let Some(pv) = vals.and_then(|v| v.get(r)) {
                            if let Some(num) = pv.as_f64() {
                                fb.append_value(num);
                            } else {
                                fb.append_null();
                            }
                        } else {
                            fb.append_null();
                        }
                    }
                    fields.push(Field::new(col_name, DataType::Float64, true));
                    arrays.push(Arc::new(fb.finish()));
                }
                DataType::Boolean => {
                    let mut bb = BooleanBuilder::with_capacity(num_rows);
                    for r in 0..num_rows {
                        if let Some(PropValue::Bool(b)) = vals.and_then(|v| v.get(r)) {
                            bb.append_value(*b);
                        } else {
                            bb.append_null();
                        }
                    }
                    fields.push(Field::new(col_name, DataType::Boolean, true));
                    arrays.push(Arc::new(bb.finish()));
                }
                _ => {
                    let mut sb = StringBuilder::with_capacity(num_rows, num_rows * 8);
                    for r in 0..num_rows {
                        match vals.and_then(|v| v.get(r)) {
                            None | Some(PropValue::Null) => sb.append_null(),
                            Some(pv) => sb.append_value(pv.as_str_cow()),
                        }
                    }
                    fields.push(Field::new(col_name, DataType::Utf8, true));
                    arrays.push(Arc::new(sb.finish()));
                }
            }
        }
    }

    let arrow_schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(arrow_schema.clone(), arrays).map_err(|e| e.to_string())?;
    let df_schema = DFSchema::try_from(arrow_schema).map_err(|e| e.to_string())?;

    let prepared_sql = normalize_sql_function_calls(&def.sql_expr);
    let ctx = shared_df_context();
    let logical_expr = ctx
        .parse_sql_expr(&prepared_sql, &df_schema)
        .map_err(|e| format!("DataFusion parse_sql_expr('{}') error: {e}", def.sql_expr))?;
    let physical_expr = ctx
        .create_physical_expr(logical_expr, &df_schema)
        .map_err(|e| format!("DataFusion create_physical_expr error: {e}"))?;

    let col_val = physical_expr
        .evaluate(&batch)
        .map_err(|e| format!("DataFusion evaluate error: {e}"))?;
    let result_arr = col_val
        .into_array(num_rows)
        .map_err(|e| format!("DataFusion into_array error: {e}"))?;

    Ok(arrow_array_to_prop_values(result_arr.as_ref()))
}

fn infer_best_arrow_type(vals: Option<&[PropValue]>) -> DataType {
    let Some(slice) = vals else {
        return DataType::Float64;
    };
    let mut saw_num = false;
    let mut saw_bool = false;
    let mut saw_non_num_str = false;

    for pv in slice {
        match pv {
            PropValue::Null => {}
            PropValue::Int(_) | PropValue::Float(_) => saw_num = true,
            PropValue::Bool(_) => saw_bool = true,
            PropValue::Str(s) => {
                if s.parse::<f64>().is_ok() {
                    saw_num = true;
                } else {
                    saw_non_num_str = true;
                }
            }
        }
    }
    if saw_non_num_str {
        DataType::Utf8
    } else if saw_num {
        DataType::Float64
    } else if saw_bool {
        DataType::Boolean
    } else {
        DataType::Float64
    }
}

pub fn arrow_array_to_prop_values(arr: &dyn ArrowArray) -> Vec<PropValue> {
    let n = arr.len();
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        if arr.is_null(i) {
            out.push(PropValue::Null);
            continue;
        }
        let pv = match arr.data_type() {
            DataType::Boolean => PropValue::Bool(arr.as_boolean().value(i)),
            DataType::Int8 => PropValue::Int(arr.as_primitive::<Int8Type>().value(i) as i64),
            DataType::Int16 => PropValue::Int(arr.as_primitive::<Int16Type>().value(i) as i64),
            DataType::Int32 => PropValue::Int(arr.as_primitive::<Int32Type>().value(i) as i64),
            DataType::Int64 => PropValue::Int(arr.as_primitive::<Int64Type>().value(i)),
            DataType::UInt8 => PropValue::Int(arr.as_primitive::<UInt8Type>().value(i) as i64),
            DataType::UInt16 => PropValue::Int(arr.as_primitive::<UInt16Type>().value(i) as i64),
            DataType::UInt32 => PropValue::Int(arr.as_primitive::<UInt32Type>().value(i) as i64),
            DataType::UInt64 => PropValue::Int(arr.as_primitive::<UInt64Type>().value(i) as i64),
            DataType::Float32 => {
                PropValue::Float(arr.as_primitive::<Float32Type>().value(i) as f64)
            }
            DataType::Float64 => PropValue::Float(arr.as_primitive::<Float64Type>().value(i)),
            _ => {
                if let Some(s) = extract_arrow_str(arr, i) {
                    PropValue::Str(s.to_string())
                } else {
                    PropValue::Null
                }
            }
        };
        out.push(pv);
    }
    out
}

/// Dictionary Lookup Table (`join_prop` -> attribute columns) with pre-indexed column set (`P2-09j`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupTableDef {
    pub name: String,
    pub join_prop: String,
    /// `join_key_value -> { attribute_col_name -> PropValue }`
    pub entries: HashMap<String, HashMap<String, PropValue>>,
    /// Pre-indexed attribute column names so lookup resolution is O(1) without scanning/cloning `entries` (`P2-09j`).
    #[serde(default)]
    pub indexed_columns: HashSet<String>,
}

impl LookupTableDef {
    pub fn new(
        name: impl Into<String>,
        join_prop: impl Into<String>,
        entries: HashMap<String, HashMap<String, PropValue>>,
    ) -> Self {
        let mut def = Self {
            name: name.into(),
            join_prop: join_prop.into(),
            entries,
            indexed_columns: HashSet::new(),
        };
        def.ensure_indexed();
        def
    }

    pub fn ensure_indexed(&mut self) {
        if self.indexed_columns.is_empty() && !self.entries.is_empty() {
            for row in self.entries.values() {
                for col in row.keys() {
                    self.indexed_columns.insert(col.clone());
                }
            }
        }
    }

    pub fn into_indexed_arc(mut self) -> Arc<Self> {
        self.ensure_indexed();
        Arc::new(self)
    }

    pub fn from_csv_bytes(
        name: impl Into<String>,
        join_prop: impl Into<String>,
        csv_bytes: &[u8],
    ) -> Result<Self, String> {
        let name = name.into();
        let join_prop = join_prop.into();
        let mut rdr = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .from_reader(csv_bytes);
        let headers: Vec<String> = rdr
            .headers()
            .map_err(|e| e.to_string())?
            .iter()
            .map(|h| h.to_string())
            .collect();
        let key_idx = headers.iter().position(|h| h == &join_prop).unwrap_or(0);

        let mut indexed_columns = HashSet::new();
        for (idx, h) in headers.iter().enumerate() {
            if idx != key_idx {
                indexed_columns.insert(h.clone());
            }
        }

        let mut entries: HashMap<String, HashMap<String, PropValue>> = HashMap::new();
        for rec in rdr.records() {
            let rec = rec.map_err(|e| e.to_string())?;
            let Some(join_val) = rec.get(key_idx) else {
                continue;
            };
            let mut attrs = HashMap::new();
            for (idx, val_str) in rec.iter().enumerate() {
                if idx == key_idx {
                    continue;
                }
                if let Some(col_name) = headers.get(idx) {
                    let pv = if let Ok(i) = val_str.parse::<i64>() {
                        PropValue::Int(i)
                    } else if let Ok(f) = val_str.parse::<f64>() {
                        PropValue::Float(f)
                    } else if val_str.eq_ignore_ascii_case("true") {
                        PropValue::Bool(true)
                    } else if val_str.eq_ignore_ascii_case("false") {
                        PropValue::Bool(false)
                    } else {
                        PropValue::Str(val_str.to_string())
                    };
                    attrs.insert(col_name.clone(), pv);
                }
            }
            entries.insert(join_val.to_string(), attrs);
        }
        Ok(Self {
            name,
            join_prop,
            entries,
            indexed_columns,
        })
    }

    pub fn from_parquet_bytes(
        name: impl Into<String>,
        join_prop: impl Into<String>,
        parquet_bytes: Bytes,
    ) -> Result<Self, String> {
        let name = name.into();
        let join_prop = join_prop.into();
        let reader = ParquetRecordBatchReaderBuilder::try_new(parquet_bytes)
            .map_err(|e| e.to_string())?
            .build()
            .map_err(|e| e.to_string())?;

        let mut entries: HashMap<String, HashMap<String, PropValue>> = HashMap::new();
        let mut indexed_columns: HashSet<String> = HashSet::new();
        for batch_res in reader {
            let batch = batch_res.map_err(|e| e.to_string())?;
            let schema = batch.schema();
            let key_idx = schema
                .fields()
                .iter()
                .position(|f| f.name() == &join_prop)
                .unwrap_or(0);

            for (i, f) in schema.fields().iter().enumerate() {
                if i != key_idx {
                    indexed_columns.insert(f.name().clone());
                }
            }

            let converted: Vec<(String, Vec<PropValue>)> = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    (
                        f.name().clone(),
                        arrow_array_to_prop_values(batch.column(i).as_ref()),
                    )
                })
                .collect();

            for row in 0..batch.num_rows() {
                let join_val = converted[key_idx].1[row].as_str_cow().into_owned();
                let mut attrs = HashMap::new();
                for (col_idx, (col_name, vals)) in converted.iter().enumerate() {
                    if col_idx != key_idx {
                        attrs.insert(col_name.clone(), vals[row].clone());
                    }
                }
                entries.insert(join_val, attrs);
            }
        }

        Ok(Self {
            name,
            join_prop,
            entries,
            indexed_columns,
        })
    }

    pub fn lookup_attr(&self, join_key: &str, attr_col: &str) -> Option<&PropValue> {
        self.entries.get(join_key)?.get(attr_col)
    }
}

/// Result of pre-evaluating `user.*` profile predicates against [`ColumnarProfileIndex`].
///
/// Handles both positive (`user.plan == "Pro"`) and negative (`user.plan != "Pro"`, `Eq(Null)`)
/// predicates in O(1) without needing the full universe of users who have never set a profile.
#[derive(Debug, Clone)]
pub enum UserFilterMask {
    /// Only canonical UIDs present in this bitmap pass.
    Include(RoaringBitmap),
    /// Every canonical UID *except* those in this bitmap passes.
    Exclude(RoaringBitmap),
}

impl UserFilterMask {
    #[inline]
    pub fn contains(&self, canon_uid: u32) -> bool {
        match self {
            Self::Include(bm) => bm.contains(canon_uid),
            Self::Exclude(bm) => !bm.contains(canon_uid),
        }
    }

    #[inline]
    pub fn is_definitely_empty(&self) -> bool {
        matches!(self, Self::Include(bm) if bm.is_empty())
    }

    pub fn intersect(self, other: Self) -> Self {
        match (self, other) {
            (Self::Include(mut a), Self::Include(b)) => {
                a &= b;
                Self::Include(a)
            }
            (Self::Include(mut a), Self::Exclude(b)) => {
                a -= b;
                Self::Include(a)
            }
            (Self::Exclude(a), Self::Include(mut b)) => {
                b -= a;
                Self::Include(b)
            }
            (Self::Exclude(mut a), Self::Exclude(b)) => {
                a |= b;
                Self::Exclude(a)
            }
        }
    }

    pub fn intersect_cohort(self, cohort: &RoaringBitmap) -> Self {
        match self {
            Self::Include(mut bm) => {
                bm &= cohort;
                Self::Include(bm)
            }
            Self::Exclude(excluded) => {
                let mut bm = cohort.clone();
                bm -= excluded;
                Self::Include(bm)
            }
        }
    }
}

const MAX_PROFILE_CAT_CARDINALITY: usize = 16_384;

/// Typed columnar representation of a single user profile property across all canonical users.
#[derive(Debug, Clone)]
enum ProfileColumn {
    /// Pure boolean property: two `RoaringBitmap`s (< 250 KB for 2M users).
    Bool {
        true_uids: RoaringBitmap,
        false_uids: RoaringBitmap,
    },
    /// Low-cardinality string property (<= 16,384 unique values):
    /// dictionary-encoded `Arc<str>` + `RoaringBitmap` inverted index + `u16` forward table.
    Categorical {
        dict: HashMap<String, u16>,
        rev_dict: Vec<Arc<str>>,
        bitmaps: Vec<RoaringBitmap>,
        /// `0` = null/absent; `v > 0` => `rev_dict[(v - 1) as usize]`.
        uid_to_val: Vec<u16>,
    },
    /// Pure numeric (`Int`/`Float`) property stored as compact `f64` + presence bitmap.
    Numeric {
        present: RoaringBitmap,
        is_int: RoaringBitmap,
        values: Vec<f64>,
    },
    /// High-cardinality (> 16,384 unique strings) or mixed-type fallback.
    Mixed { values: HashMap<u32, PropValue> },
}

impl ProfileColumn {
    fn from_first(uid: u32, val: PropValue) -> Self {
        let mut col = match val {
            PropValue::Bool(_) => Self::Bool {
                true_uids: RoaringBitmap::new(),
                false_uids: RoaringBitmap::new(),
            },
            PropValue::Str(_) => Self::Categorical {
                dict: HashMap::new(),
                rev_dict: Vec::new(),
                bitmaps: Vec::new(),
                uid_to_val: Vec::new(),
            },
            PropValue::Int(_) | PropValue::Float(_) => Self::Numeric {
                present: RoaringBitmap::new(),
                is_int: RoaringBitmap::new(),
                values: Vec::new(),
            },
            PropValue::Null => Self::Mixed {
                values: HashMap::new(),
            },
        };
        col.set(uid, val);
        col
    }

    fn promote_to_mixed(&mut self) {
        if matches!(self, Self::Mixed { .. }) {
            return;
        }
        let mut values = HashMap::new();
        match self {
            Self::Bool {
                true_uids,
                false_uids,
            } => {
                for u in true_uids.iter() {
                    values.insert(u, PropValue::Bool(true));
                }
                for u in false_uids.iter() {
                    values.insert(u, PropValue::Bool(false));
                }
            }
            Self::Categorical {
                rev_dict,
                uid_to_val,
                ..
            } => {
                for (u, &code) in uid_to_val.iter().enumerate() {
                    if code > 0 {
                        if let Some(s) = rev_dict.get((code - 1) as usize) {
                            values.insert(u as u32, PropValue::Str(s.to_string()));
                        }
                    }
                }
            }
            Self::Numeric {
                present,
                is_int,
                values: nums,
            } => {
                for u in present.iter() {
                    let v = nums[u as usize];
                    if is_int.contains(u) {
                        values.insert(u, PropValue::Int(v as i64));
                    } else {
                        values.insert(u, PropValue::Float(v));
                    }
                }
            }
            Self::Mixed { .. } => return,
        }
        *self = Self::Mixed { values };
    }

    fn remove(&mut self, uid: u32) {
        match self {
            Self::Bool {
                true_uids,
                false_uids,
            } => {
                true_uids.remove(uid);
                false_uids.remove(uid);
            }
            Self::Categorical {
                bitmaps,
                uid_to_val,
                ..
            } => {
                if let Some(slot) = uid_to_val.get_mut(uid as usize) {
                    let prev = *slot;
                    if prev > 0 {
                        if let Some(bm) = bitmaps.get_mut((prev - 1) as usize) {
                            bm.remove(uid);
                        }
                        *slot = 0;
                    }
                }
            }
            Self::Numeric {
                present,
                is_int,
                values,
            } => {
                if present.remove(uid) {
                    is_int.remove(uid);
                    if let Some(slot) = values.get_mut(uid as usize) {
                        *slot = f64::NAN;
                    }
                }
            }
            Self::Mixed { values } => {
                values.remove(&uid);
            }
        }
    }

    fn set(&mut self, uid: u32, val: PropValue) {
        if matches!(val, PropValue::Null) {
            self.remove(uid);
            return;
        }

        // Check whether the incoming value matches the current column specialization.
        let compatible = matches!(
            (&self, &val),
            (Self::Bool { .. }, PropValue::Bool(_))
                | (Self::Categorical { .. }, PropValue::Str(_))
                | (
                    Self::Numeric { .. },
                    PropValue::Int(_) | PropValue::Float(_)
                )
                | (Self::Mixed { .. }, _)
        );
        if !compatible {
            self.promote_to_mixed();
        }

        match (self, val) {
            (
                Self::Bool {
                    true_uids,
                    false_uids,
                },
                PropValue::Bool(b),
            ) => {
                if b {
                    false_uids.remove(uid);
                    true_uids.insert(uid);
                } else {
                    true_uids.remove(uid);
                    false_uids.insert(uid);
                }
            }
            (col @ Self::Categorical { .. }, PropValue::Str(s)) => {
                let need_promote = if let Self::Categorical { dict, .. } = col {
                    !dict.contains_key(&s) && dict.len() >= MAX_PROFILE_CAT_CARDINALITY
                } else {
                    false
                };
                if need_promote {
                    col.promote_to_mixed();
                    if let Self::Mixed { values } = col {
                        values.insert(uid, PropValue::Str(s));
                    }
                    return;
                }
                if let Self::Categorical {
                    dict,
                    rev_dict,
                    bitmaps,
                    uid_to_val,
                } = col
                {
                    let idx = if let Some(&existing) = dict.get(&s) {
                        existing
                    } else {
                        let next = rev_dict.len() as u16;
                        rev_dict.push(Arc::from(s.as_str()));
                        bitmaps.push(RoaringBitmap::new());
                        dict.insert(s, next);
                        next
                    };
                    let u = uid as usize;
                    if uid_to_val.len() <= u {
                        uid_to_val.resize(u + 1, 0);
                    }
                    let prev = uid_to_val[u];
                    if prev > 0 && (prev - 1) != idx {
                        bitmaps[(prev - 1) as usize].remove(uid);
                    }
                    uid_to_val[u] = idx + 1;
                    bitmaps[idx as usize].insert(uid);
                }
            }
            (
                Self::Numeric {
                    present,
                    is_int,
                    values,
                },
                PropValue::Int(i),
            ) => {
                let u = uid as usize;
                if values.len() <= u {
                    values.resize(u + 1, f64::NAN);
                }
                values[u] = i as f64;
                present.insert(uid);
                is_int.insert(uid);
            }
            (
                Self::Numeric {
                    present,
                    is_int,
                    values,
                },
                PropValue::Float(f),
            ) => {
                let u = uid as usize;
                if values.len() <= u {
                    values.resize(u + 1, f64::NAN);
                }
                values[u] = f;
                present.insert(uid);
                is_int.remove(uid);
            }
            (Self::Mixed { values }, v) => {
                values.insert(uid, v);
            }
            _ => {}
        }
    }

    #[inline]
    fn get(&self, uid: u32) -> PropValue {
        match self {
            Self::Bool {
                true_uids,
                false_uids,
            } => {
                if true_uids.contains(uid) {
                    PropValue::Bool(true)
                } else if false_uids.contains(uid) {
                    PropValue::Bool(false)
                } else {
                    PropValue::Null
                }
            }
            Self::Categorical {
                rev_dict,
                uid_to_val,
                ..
            } => {
                let code = uid_to_val.get(uid as usize).copied().unwrap_or(0);
                if code == 0 {
                    PropValue::Null
                } else {
                    PropValue::Str(rev_dict[(code - 1) as usize].to_string())
                }
            }
            Self::Numeric {
                present,
                is_int,
                values,
            } => {
                if !present.contains(uid) {
                    PropValue::Null
                } else {
                    let v = values[uid as usize];
                    if is_int.contains(uid) {
                        PropValue::Int(v as i64)
                    } else {
                        PropValue::Float(v)
                    }
                }
            }
            Self::Mixed { values } => values.get(&uid).cloned().unwrap_or(PropValue::Null),
        }
    }

    #[inline]
    fn get_str_arc(&self, uid: u32) -> Option<Arc<str>> {
        if let Self::Categorical {
            rev_dict,
            uid_to_val,
            ..
        } = self
        {
            let code = uid_to_val.get(uid as usize).copied().unwrap_or(0);
            if code > 0 {
                return Some(rev_dict[(code - 1) as usize].clone());
            }
        }
        None
    }

    #[inline]
    fn get_f64(&self, uid: u32) -> Option<f64> {
        match self {
            Self::Numeric {
                present, values, ..
            } => present.contains(uid).then(|| values[uid as usize]),
            _ => self.get(uid).as_f64(),
        }
    }

    fn present_bitmap(&self) -> RoaringBitmap {
        match self {
            Self::Bool {
                true_uids,
                false_uids,
            } => true_uids | false_uids,
            Self::Categorical { bitmaps, .. } => {
                let mut out = RoaringBitmap::new();
                for bm in bitmaps {
                    out |= bm;
                }
                out
            }
            Self::Numeric { present, .. } => present.clone(),
            Self::Mixed { values } => values.keys().copied().collect(),
        }
    }

    /// Evaluates `pred` against every user in this column and returns an exact [`UserFilterMask`].
    fn evaluate_predicate(&self, pred: &PropPredicate) -> UserFilterMask {
        match &pred.filter {
            PropFilterOp::IsSet | PropFilterOp::Neq(PropValue::Null) => {
                UserFilterMask::Include(self.present_bitmap())
            }
            PropFilterOp::Eq(PropValue::Null) => UserFilterMask::Exclude(self.present_bitmap()),
            PropFilterOp::Eq(target) => UserFilterMask::Include(self.matching_non_null_eq(target)),
            PropFilterOp::Neq(target) => UserFilterMask::Exclude(self.matching_non_null_eq(target)),
            PropFilterOp::In(options) => {
                let mut out = RoaringBitmap::new();
                match self {
                    Self::Categorical { dict, bitmaps, .. } => {
                        for opt in options {
                            if let Some(&idx) = dict.get(opt) {
                                out |= &bitmaps[idx as usize];
                            }
                        }
                    }
                    Self::Bool {
                        true_uids,
                        false_uids,
                    } => {
                        if options.iter().any(|o| o == "true") {
                            out |= true_uids;
                        }
                        if options.iter().any(|o| o == "false") {
                            out |= false_uids;
                        }
                    }
                    Self::Numeric {
                        present,
                        is_int,
                        values,
                    } => {
                        for u in present.iter() {
                            let pv = if is_int.contains(u) {
                                PropValue::Int(values[u as usize] as i64)
                            } else {
                                PropValue::Float(values[u as usize])
                            };
                            if pred.matches(Some(&pv)) {
                                out.insert(u);
                            }
                        }
                    }
                    Self::Mixed { values } => {
                        for (&u, pv) in values {
                            if pred.matches(Some(pv)) {
                                out.insert(u);
                            }
                        }
                    }
                }
                UserFilterMask::Include(out)
            }
            PropFilterOp::Contains(sub) => {
                let mut out = RoaringBitmap::new();
                match self {
                    Self::Categorical {
                        rev_dict, bitmaps, ..
                    } => {
                        for (idx, s) in rev_dict.iter().enumerate() {
                            if s.contains(sub.as_str()) {
                                out |= &bitmaps[idx];
                            }
                        }
                    }
                    Self::Bool {
                        true_uids,
                        false_uids,
                    } => {
                        if "true".contains(sub.as_str()) {
                            out |= true_uids;
                        }
                        if "false".contains(sub.as_str()) {
                            out |= false_uids;
                        }
                    }
                    Self::Numeric {
                        present,
                        is_int,
                        values,
                    } => {
                        for u in present.iter() {
                            let pv = if is_int.contains(u) {
                                PropValue::Int(values[u as usize] as i64)
                            } else {
                                PropValue::Float(values[u as usize])
                            };
                            if pred.matches(Some(&pv)) {
                                out.insert(u);
                            }
                        }
                    }
                    Self::Mixed { values } => {
                        for (&u, pv) in values {
                            if pred.matches(Some(pv)) {
                                out.insert(u);
                            }
                        }
                    }
                }
                UserFilterMask::Include(out)
            }
            PropFilterOp::Gt(b)
            | PropFilterOp::Gte(b)
            | PropFilterOp::Lt(b)
            | PropFilterOp::Lte(b) => {
                let bound = *b;
                let cmp = |v: f64| match pred.filter {
                    PropFilterOp::Gt(_) => v > bound,
                    PropFilterOp::Gte(_) => v >= bound,
                    PropFilterOp::Lt(_) => v < bound,
                    PropFilterOp::Lte(_) => v <= bound,
                    _ => false,
                };
                let mut out = RoaringBitmap::new();
                match self {
                    Self::Numeric {
                        present, values, ..
                    } => {
                        for u in present.iter() {
                            if cmp(values[u as usize]) {
                                out.insert(u);
                            }
                        }
                    }
                    Self::Categorical {
                        rev_dict, bitmaps, ..
                    } => {
                        for (idx, s) in rev_dict.iter().enumerate() {
                            if s.trim().parse::<f64>().is_ok_and(cmp) {
                                out |= &bitmaps[idx];
                            }
                        }
                    }
                    Self::Bool { .. } => {}
                    Self::Mixed { values } => {
                        for (&u, pv) in values {
                            if pv.as_f64().is_some_and(cmp) {
                                out.insert(u);
                            }
                        }
                    }
                }
                UserFilterMask::Include(out)
            }
        }
    }

    fn matching_non_null_eq(&self, target: &PropValue) -> RoaringBitmap {
        let eq_pred = PropPredicate {
            prop_name: String::new(),
            filter: PropFilterOp::Eq(target.clone()),
        };
        match (self, target) {
            (
                Self::Bool {
                    true_uids,
                    false_uids,
                },
                PropValue::Bool(b),
            ) => {
                if *b {
                    true_uids.clone()
                } else {
                    false_uids.clone()
                }
            }
            (
                Self::Categorical {
                    dict,
                    rev_dict,
                    bitmaps,
                    ..
                },
                PropValue::Str(s),
            ) => {
                if s.trim().parse::<f64>().is_err() {
                    dict.get(s)
                        .map(|&idx| bitmaps[idx as usize].clone())
                        .unwrap_or_default()
                } else {
                    let mut out = RoaringBitmap::new();
                    for (idx, val_s) in rev_dict.iter().enumerate() {
                        if eq_pred.matches(Some(&PropValue::Str(val_s.to_string()))) {
                            out |= &bitmaps[idx];
                        }
                    }
                    out
                }
            }
            (
                Self::Categorical {
                    rev_dict, bitmaps, ..
                },
                _,
            ) => {
                let mut out = RoaringBitmap::new();
                for (idx, val_s) in rev_dict.iter().enumerate() {
                    if eq_pred.matches(Some(&PropValue::Str(val_s.to_string()))) {
                        out |= &bitmaps[idx];
                    }
                }
                out
            }
            (
                Self::Numeric {
                    present,
                    is_int,
                    values,
                },
                _,
            ) => {
                let mut out = RoaringBitmap::new();
                if let Some(target_f) = target.as_f64() {
                    for u in present.iter() {
                        if (values[u as usize] - target_f).abs() < 1e-12 {
                            out.insert(u);
                        }
                    }
                } else {
                    for u in present.iter() {
                        let pv = if is_int.contains(u) {
                            PropValue::Int(values[u as usize] as i64)
                        } else {
                            PropValue::Float(values[u as usize])
                        };
                        if eq_pred.matches(Some(&pv)) {
                            out.insert(u);
                        }
                    }
                }
                out
            }
            (Self::Bool { .. }, _) => RoaringBitmap::new(),
            (Self::Mixed { values }, _) => {
                let mut out = RoaringBitmap::new();
                for (&u, pv) in values {
                    if eq_pred.matches(Some(pv)) {
                        out.insert(u);
                    }
                }
                out
            }
        }
    }
}

/// Columnar, inverted-indexed in-memory store for analytical user profile properties.
///
/// Replaces `HashMap<u32, HashMap<String, PropValue>>`, cutting RAM ~40× at 2M users and
/// answering `user.*` equality/boolean/numeric filters as `RoaringBitmap`s in < 1 ms.
#[derive(Debug, Default)]
struct ColumnarProfileIndex {
    columns: HashMap<String, ProfileColumn>,
    active_users: RoaringBitmap,
}

impl ColumnarProfileIndex {
    fn set_all(&mut self, profiles: HashMap<u32, HashMap<String, PropValue>>) {
        self.columns.clear();
        self.active_users.clear();
        for (uid, props) in profiles {
            self.active_users.insert(uid);
            for (k, v) in props {
                if matches!(v, PropValue::Null) {
                    continue;
                }
                match self.columns.get_mut(&k) {
                    Some(col) => col.set(uid, v),
                    None => {
                        self.columns.insert(k, ProfileColumn::from_first(uid, v));
                    }
                }
            }
        }
    }

    fn upsert(&mut self, uid: u32, props: HashMap<String, PropValue>) {
        // Clear any previously set keys for this user that are absent from the new profile snapshot.
        if self.active_users.contains(uid) {
            for (k, col) in &mut self.columns {
                if !props.contains_key(k) {
                    col.remove(uid);
                }
            }
        }
        self.active_users.insert(uid);
        for (k, v) in props {
            if matches!(v, PropValue::Null) {
                if let Some(col) = self.columns.get_mut(&k) {
                    col.remove(uid);
                }
                continue;
            }
            match self.columns.get_mut(&k) {
                Some(col) => col.set(uid, v),
                None => {
                    self.columns.insert(k, ProfileColumn::from_first(uid, v));
                }
            }
        }
    }

    fn remove(&mut self, uid: u32) {
        if self.active_users.remove(uid) {
            for col in self.columns.values_mut() {
                col.remove(uid);
            }
        }
    }

    fn evaluate_predicate(&self, pred: &PropPredicate) -> UserFilterMask {
        let key = pred
            .prop_name
            .strip_prefix("user.")
            .unwrap_or(&pred.prop_name);
        match self.columns.get(key) {
            Some(col) => col.evaluate_predicate(pred),
            None => {
                // Property never set on any user: matches only if `None` passes the predicate.
                if pred.matches(None) {
                    UserFilterMask::Exclude(RoaringBitmap::new())
                } else {
                    UserFilterMask::Include(RoaringBitmap::new())
                }
            }
        }
    }
}

/// Pre-built `ColumnarProfileIndex` and property metadata constructed off-lock before being
/// atomically installed into [`VirtualCatalog`] under [`VirtualCatalog::acquire_refresh_write`].
pub struct PreparedUserProfiles {
    index: ColumnarProfileIndex,
    timestamps: HashMap<u32, i64>,
    prop_keys: HashSet<String>,
}

/// Per-project virtual schema catalog (Custom Events, Custom Properties, Lookup Tables, Saved Cohorts, User Profiles, and IANA Timezone).
pub struct VirtualCatalog {
    pub custom_events: DashMap<String, CustomEventDef>,
    pub custom_properties: DashMap<String, CustomPropertyDef>,
    pub lookup_tables: DashMap<String, Arc<LookupTableDef>>,
    pub saved_cohorts: DashMap<String, BehavioralCohortDef>,
    user_profiles: RwLock<ColumnarProfileIndex>,
    user_profile_ts: RwLock<HashMap<u32, i64>>,
    user_prop_keys: RwLock<HashSet<String>>,
    timezone: RwLock<Tz>,
    query_refresh_gate: tokio::sync::RwLock<()>,
}

impl Default for VirtualCatalog {
    fn default() -> Self {
        Self {
            custom_events: DashMap::new(),
            custom_properties: DashMap::new(),
            lookup_tables: DashMap::new(),
            saved_cohorts: DashMap::new(),
            user_profiles: RwLock::new(ColumnarProfileIndex::default()),
            user_profile_ts: RwLock::new(HashMap::new()),
            user_prop_keys: RwLock::new(HashSet::new()),
            timezone: RwLock::new(chrono_tz::UTC),
            query_refresh_gate: tokio::sync::RwLock::new(()),
        }
    }
}

impl VirtualCatalog {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub async fn acquire_query_read(&self) -> tokio::sync::RwLockReadGuard<'_, ()> {
        self.query_refresh_gate.read().await
    }

    pub async fn acquire_refresh_write(&self) -> tokio::sync::RwLockWriteGuard<'_, ()> {
        self.query_refresh_gate.write().await
    }

    pub fn timezone(&self) -> Tz {
        *self.timezone.read()
    }

    pub fn set_timezone(&self, tz_name: &str) {
        if let Ok(parsed) = tz_name.trim().parse::<Tz>() {
            *self.timezone.write() = parsed;
        }
    }

    pub fn insert_lookup_table(&self, def: LookupTableDef) {
        let name = def.name.clone();
        self.lookup_tables.insert(name, def.into_indexed_arc());
    }

    pub fn prepare_user_profiles(
        profiles: HashMap<u32, HashMap<String, PropValue>>,
        timestamps: HashMap<u32, i64>,
    ) -> PreparedUserProfiles {
        let mut prop_keys = HashSet::new();
        for p in profiles.values() {
            for k in p.keys() {
                prop_keys.insert(k.clone());
            }
        }
        let mut index = ColumnarProfileIndex::default();
        index.set_all(profiles);
        PreparedUserProfiles {
            index,
            timestamps,
            prop_keys,
        }
    }

    pub fn install_prepared_user_profiles(&self, prepared: PreparedUserProfiles) {
        *self.user_prop_keys.write() = prepared.prop_keys;
        let mut idx = self.user_profiles.write();
        let mut ts_map = self.user_profile_ts.write();
        *idx = prepared.index;
        *ts_map = prepared.timestamps;
    }

    pub fn set_user_profiles(&self, profiles: HashMap<u32, HashMap<String, PropValue>>) {
        self.set_user_profiles_with_ts(profiles, HashMap::new());
    }

    pub fn set_user_profiles_with_ts(
        &self,
        profiles: HashMap<u32, HashMap<String, PropValue>>,
        timestamps: HashMap<u32, i64>,
    ) {
        self.install_prepared_user_profiles(Self::prepare_user_profiles(profiles, timestamps));
    }

    pub fn upsert_user_profile(&self, canon_uid: u32, props: HashMap<String, PropValue>) {
        {
            let mut keys = self.user_prop_keys.write();
            for k in props.keys() {
                keys.insert(k.clone());
            }
        }
        let mut idx = self.user_profiles.write();
        let mut ts_map = self.user_profile_ts.write();
        idx.upsert(canon_uid, props);
        ts_map.remove(&canon_uid);
    }

    pub fn upsert_user_profile_with_ts(
        &self,
        canon_uid: u32,
        props: HashMap<String, PropValue>,
        updated_at_ms: i64,
    ) {
        {
            let mut keys = self.user_prop_keys.write();
            for k in props.keys() {
                keys.insert(k.clone());
            }
        }
        let mut idx = self.user_profiles.write();
        let mut ts_map = self.user_profile_ts.write();
        idx.upsert(canon_uid, props);
        ts_map.insert(canon_uid, updated_at_ms);
    }

    pub fn remove_user_profile(&self, canon_uid: u32) {
        let mut idx = self.user_profiles.write();
        let mut ts_map = self.user_profile_ts.write();
        idx.remove(canon_uid);
        ts_map.remove(&canon_uid);
    }

    /// Evicts any cached profile entries whose `(uid, updated_at_ms)` (or resolved canonical UID)
    /// is hidden by `expanded`. Entries without a recorded timestamp default to `0` (fail-closed).
    pub fn evict_erased_profiles(
        &self,
        expanded: &pulse_storage::ErasedUids,
        id_snap: Option<&IdentitySnapshot>,
    ) -> Vec<u32> {
        if expanded.is_empty() {
            return Vec::new();
        }
        let any_hidden = {
            let idx = self.user_profiles.read();
            if idx.active_users.is_empty() {
                return Vec::new();
            }
            let ts_map = self.user_profile_ts.read();
            idx.active_users.iter().any(|cat_uid| {
                let prof_ts = ts_map.get(&cat_uid).copied().unwrap_or(0);
                let resolved = id_snap.map_or(cat_uid, |s| s.resolve(cat_uid));
                expanded.hides(cat_uid, prof_ts) || expanded.hides(resolved, prof_ts)
            })
        };
        if !any_hidden {
            return Vec::new();
        }
        let mut idx = self.user_profiles.write();
        let mut ts_map = self.user_profile_ts.write();
        let to_evict: Vec<u32> = idx
            .active_users
            .iter()
            .filter(|&cat_uid| {
                let prof_ts = ts_map.get(&cat_uid).copied().unwrap_or(0);
                let resolved = id_snap.map_or(cat_uid, |s| s.resolve(cat_uid));
                expanded.hides(cat_uid, prof_ts) || expanded.hides(resolved, prof_ts)
            })
            .collect();
        for &uid in &to_evict {
            idx.remove(uid);
            ts_map.remove(&uid);
        }
        to_evict
    }

    /// Sorted user profile property keys (without the `user.` prefix).
    pub fn user_prop_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.user_prop_keys.read().iter().cloned().collect();
        keys.sort();
        keys
    }

    pub fn has_user_prop(&self, prop_name: &str) -> bool {
        let key = prop_name.strip_prefix("user.").unwrap_or(prop_name);
        self.user_prop_keys.read().contains(key)
    }

    /// Pre-evaluates a slice of `user.*` predicates directly against the columnar profile index,
    /// returning a single combined [`UserFilterMask`] without touching any event segments.
    pub fn evaluate_user_predicates(&self, preds: &[PropPredicate]) -> Option<UserFilterMask> {
        if preds.is_empty() {
            return None;
        }
        let idx = self.user_profiles.read();
        let mut combined: Option<UserFilterMask> = None;
        for pred in preds {
            let mask = idx.evaluate_predicate(pred);
            combined = Some(match combined {
                Some(prev) => prev.intersect(mask),
                None => mask,
            });
            if combined
                .as_ref()
                .is_some_and(UserFilterMask::is_definitely_empty)
            {
                break;
            }
        }
        combined
    }

    /// Point lookup of a single user property for one user (zero full-column materialization).
    pub fn get_user_prop(&self, canon_uid: u32, raw_uid: u32, prop_name: &str) -> PropValue {
        let key = prop_name.strip_prefix("user.").unwrap_or(prop_name);
        let idx = self.user_profiles.read();
        let Some(col) = idx.columns.get(key) else {
            return PropValue::Null;
        };
        let v = col.get(canon_uid);
        if matches!(v, PropValue::Null) && raw_uid != canon_uid {
            col.get(raw_uid)
        } else {
            v
        }
    }

    /// Zero-allocation string lookup for categorical user properties (used in breakdowns).
    pub fn get_user_prop_str_arc(
        &self,
        canon_uid: u32,
        raw_uid: u32,
        prop_name: &str,
    ) -> Option<Arc<str>> {
        let key = prop_name.strip_prefix("user.").unwrap_or(prop_name);
        let idx = self.user_profiles.read();
        let col = idx.columns.get(key)?;
        col.get_str_arc(canon_uid).or_else(|| {
            (raw_uid != canon_uid)
                .then(|| col.get_str_arc(raw_uid))
                .flatten()
        })
    }

    /// Direct numeric lookup for user properties (used in PropSum/PropAvg/etc.).
    pub fn get_user_prop_f64(&self, canon_uid: u32, raw_uid: u32, prop_name: &str) -> Option<f64> {
        let key = prop_name.strip_prefix("user.").unwrap_or(prop_name);
        let idx = self.user_profiles.read();
        let col = idx.columns.get(key)?;
        col.get_f64(canon_uid).or_else(|| {
            (raw_uid != canon_uid)
                .then(|| col.get_f64(raw_uid))
                .flatten()
        })
    }

    pub fn resolve_user_prop_column(
        &self,
        raw_uids: &[u32],
        id_snap: Option<&IdentitySnapshot>,
        prop_name: &str,
    ) -> Vec<PropValue> {
        let key = prop_name.strip_prefix("user.").unwrap_or(prop_name);
        let idx = self.user_profiles.read();
        let Some(col) = idx.columns.get(key) else {
            return vec![PropValue::Null; raw_uids.len()];
        };
        raw_uids
            .iter()
            .map(|&raw| {
                let canon = id_snap.map(|s| s.resolve(raw)).unwrap_or(raw);
                let v = col.get(canon);
                if matches!(v, PropValue::Null) && raw != canon {
                    col.get(raw)
                } else {
                    v
                }
            })
            .collect()
    }

    /// Resolves `(Arc<LookupTableDef>, attr_col)` strictly for qualified `"table.col"` names in O(1) (`P2-09j`).
    /// Never matches unqualified column names so lookup tables can NEVER silently shadow real event properties!
    pub fn resolve_lookup_column(&self, prop_name: &str) -> Option<(Arc<LookupTableDef>, String)> {
        let (tbl, col) = prop_name.split_once('.')?;
        let def = self.lookup_tables.get(tbl)?;
        if def.indexed_columns.contains(col)
            || (def.indexed_columns.is_empty() && def.entries.values().any(|m| m.contains_key(col)))
        {
            Some((def.clone(), col.to_string()))
        } else {
            None
        }
    }
}

/// Multi-project catalog registry shared between Control Plane and Query Engine.
#[derive(Default)]
pub struct CatalogRegistry {
    projects: DashMap<String, Arc<VirtualCatalog>>,
}

impl CatalogRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn for_project(&self, project_id: &str) -> Arc<VirtualCatalog> {
        self.projects
            .entry(project_id.to_string())
            .or_default()
            .clone()
    }

    pub fn get_project(&self, project_id: &str) -> Option<Arc<VirtualCatalog>> {
        self.projects.get(project_id).map(|e| e.value().clone())
    }
}
