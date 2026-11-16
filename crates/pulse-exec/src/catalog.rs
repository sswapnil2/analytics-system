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
use dashmap::DashMap;
use datafusion::common::DFSchema;
use datafusion::prelude::SessionContext;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pulse_storage::extract_arrow_str;
use pulse_wire::PropValue;
use serde::{Deserialize, Serialize};

use crate::ast::{BehavioralCohortDef, PropPredicate};

fn shared_df_context() -> &'static SessionContext {
    static CTX: OnceLock<SessionContext> = OnceLock::new();
    CTX.get_or_init(SessionContext::new)
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
    /// SQL expression evaluated via DataFusion, e.g. `CASE WHEN amount > 1000 THEN 'High' ELSE 'Standard' END` or `amount * 0.018`.
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

/// Extracts candidate column identifiers from an inline SQL expression, excluding SQL keywords.
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
                    break;
                }
                i += 1;
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
            // Skip function calls followed immediately by `(`
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

    let ctx = shared_df_context();
    let logical_expr = ctx
        .parse_sql_expr(&def.sql_expr, &df_schema)
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

/// Dictionary Lookup Table (`join_prop` -> attribute columns) importable from CSV, Parquet, or JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupTableDef {
    pub name: String,
    pub join_prop: String,
    /// `join_key_value -> { attribute_col_name -> PropValue }`
    pub entries: HashMap<String, HashMap<String, PropValue>>,
}

impl LookupTableDef {
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
        for batch_res in reader {
            let batch = batch_res.map_err(|e| e.to_string())?;
            let schema = batch.schema();
            let key_idx = schema
                .fields()
                .iter()
                .position(|f| f.name() == &join_prop)
                .unwrap_or(0);

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
        })
    }

    pub fn lookup_attr(&self, join_key: &str, attr_col: &str) -> Option<&PropValue> {
        self.entries.get(join_key)?.get(attr_col)
    }
}

/// Per-project virtual schema catalog (Custom Events, Custom Properties, Lookup Tables, Saved Cohorts).
#[derive(Default)]
pub struct VirtualCatalog {
    pub custom_events: DashMap<String, CustomEventDef>,
    pub custom_properties: DashMap<String, CustomPropertyDef>,
    pub lookup_tables: DashMap<String, LookupTableDef>,
    pub saved_cohorts: DashMap<String, BehavioralCohortDef>,
}

impl VirtualCatalog {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Resolves `(lookup_table_name, attr_col)` for either `"table.col"` or an unambiguous `"col"`.
    pub fn resolve_lookup_column(&self, prop_name: &str) -> Option<(LookupTableDef, String)> {
        if let Some((tbl, col)) = prop_name.split_once('.') {
            if let Some(def) = self.lookup_tables.get(tbl) {
                return Some((def.clone(), col.to_string()));
            }
        }
        for entry in self.lookup_tables.iter() {
            if entry
                .value()
                .entries
                .values()
                .any(|m| m.contains_key(prop_name))
            {
                return Some((entry.value().clone(), prop_name.to_string()));
            }
        }
        None
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
}
