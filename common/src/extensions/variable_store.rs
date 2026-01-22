use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::execution::TaskContext;
use datafusion::prelude::SessionConfig;
use datafusion::variable::{VarProvider, VarType};
use rustc_hash::FxHashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard};

// TODO: remove and use the var_provider.rs from DataFusion expr
#[derive(Debug)]
pub struct VariableStore {
    kind: VarType,
    variables: RwLock<FxHashMap<String, String>>,
}

#[derive(Default)]
pub struct VariableStoreExtension {
    stores: FxHashMap<VarType, Arc<VariableStore>>,
}

impl VariableStoreExtension {
    pub fn set_store(&mut self, store: Arc<VariableStore>) {
        self.stores.insert(store.kind.clone(), store);
    }

    pub fn set_variables(&mut self, kind: VarType, variables: FxHashMap<String, String>) {
        self.set_store(VariableStore::new(kind, variables))
    }

    pub fn store(&self, kind: VarType) -> Arc<VariableStore> {
        self.stores.get(&kind).unwrap().clone()
    }

    pub fn store_opt(&self, kind: VarType) -> Option<Arc<VariableStore>> {
        self.stores.get(&kind).cloned()
    }
}

impl VariableStore {
    pub fn new(kind: VarType, initial_variables: FxHashMap<String, String>) -> Arc<Self> {
        Arc::new(Self {
            kind,
            variables: RwLock::new(initial_variables),
        })
    }

    pub fn set_variable(self: Arc<Self>, name: &str, value: &str) {
        self.variables
            .write()
            .unwrap()
            .insert(name.to_string(), value.to_string());
    }

    pub fn get_variable(&self, name: &str) -> Option<String> {
        self.variables.read().unwrap().get(name).cloned()
    }

    pub fn read(&'_ self) -> RwLockReadGuard<'_, FxHashMap<String, String>> {
        self.variables.read().unwrap()
    }
}

fn extract_variable_name(var_names: &[String]) -> Option<String> {
    if var_names[0] == "@@global" || var_names[0] == "@@session" {
        var_names.get(1).cloned()
    } else {
        let mut var_name = var_names.first().cloned()?;
        while let Some(vn) = var_name.strip_prefix('@') {
            var_name = vn.to_string();
        }

        Some(var_name)
    }
}

impl VarProvider for VariableStore {
    fn get_value(&self, var_names: Vec<String>) -> datafusion::common::Result<ScalarValue> {
        let Some(var_name) = extract_variable_name(&var_names) else {
            return Err(DataFusionError::Execution(
                "Empty variable name".to_string(),
            ));
        };

        if let Some(v) = self.variables.read().unwrap().get(&var_name) {
            Ok(ScalarValue::Utf8(Some(v.to_string())))
        } else {
            Err(DataFusionError::Execution(format!(
                "Variable {} not found",
                var_name
            )))
        }
    }

    fn get_type(&self, var_names: &[String]) -> Option<DataType> {
        let var_name = extract_variable_name(var_names)?;
        if self.variables.read().unwrap().contains_key(&var_name) {
            Some(DataType::Utf8)
        } else {
            None
        }
    }
}

pub trait VariableStoreGetter {
    fn get_variable_store_extension(&self) -> Option<Arc<VariableStoreExtension>>;

    fn get_variable_store(&self, var_type: VarType) -> Option<Arc<VariableStore>> {
        let ext = self.get_variable_store_extension()?;
        ext.store_opt(var_type)
    }
}

impl VariableStoreGetter for &SessionConfig {
    fn get_variable_store_extension(&self) -> Option<Arc<VariableStoreExtension>> {
        self.get_extension()
    }
}

impl VariableStoreGetter for Arc<TaskContext> {
    fn get_variable_store_extension(&self) -> Option<Arc<VariableStoreExtension>> {
        self.session_config().get_variable_store_extension()
    }
}
