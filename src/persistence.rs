use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::ui::{ComputedColumn, ComputedColumnType};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedComputedColumn {
    pub name: String,
    pub expression: String,
    pub column_type: PersistedComputedColumnType,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PersistedComputedColumnType {
    Aggregate(String),
    RowOperation(Vec<String>),
    MixedOperation(Vec<String>, Vec<String>),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileComputedColumns {
    pub file_path: String,
    pub file_hash: String, // Simple hash to detect file changes
    pub last_modified: u64, // Unix timestamp
    pub computed_columns: HashMap<String, Vec<PersistedComputedColumn>>, // table_name -> columns
}

pub struct ComputedColumnPersistence {
    storage_path: PathBuf,
}

impl ComputedColumnPersistence {
    pub fn new() -> Result<Self> {
        let storage_path = get_storage_path()?;
        Ok(Self { storage_path })
    }

    pub fn save_computed_columns(
        &self,
        file_path: &str,
        table_name: &str,
        computed_columns: &[ComputedColumn],
    ) -> Result<()> {
        let file_hash = self.calculate_file_hash(file_path)?;
        let mut file_data = self.load_file_data(file_path).unwrap_or_else(|_| {
            let last_modified = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            FileComputedColumns {
                file_path: file_path.to_string(),
                file_hash: file_hash.clone(),
                last_modified,
                computed_columns: HashMap::new(),
            }
        });

        // Update file data
        file_data.file_hash = file_hash;
        file_data.last_modified = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs();

        // Convert and store computed columns
        let persisted_columns: Vec<PersistedComputedColumn> = computed_columns
            .iter()
            .map(|col| PersistedComputedColumn {
                name: col.name.clone(),
                expression: col.expression.clone(),
                column_type: match &col.column_type {
                    ComputedColumnType::Aggregate(func) => PersistedComputedColumnType::Aggregate(func.clone()),
                    ComputedColumnType::RowOperation(cols) => PersistedComputedColumnType::RowOperation(cols.clone()),
                    ComputedColumnType::MixedOperation(cols, aggs) => PersistedComputedColumnType::MixedOperation(cols.clone(), aggs.clone()),
                },
            })
            .collect();

        file_data.computed_columns.insert(table_name.to_string(), persisted_columns);

        // Save to file
        let storage_file = self.get_storage_file_path(file_path);
        let json = serde_json::to_string_pretty(&file_data)
            .context("Failed to serialize computed columns")?;
        fs::write(&storage_file, json)
            .context("Failed to write computed columns file")?;

        Ok(())
    }

    pub fn load_computed_columns(
        &self,
        file_path: &str,
        table_name: &str,
    ) -> Result<Vec<ComputedColumn>> {
        let file_data = self.load_file_data(file_path)?;
        
        // Check if file has been modified since last save
        let current_hash = self.calculate_file_hash(file_path)?;
        if current_hash != file_data.file_hash {
            // File has changed, return empty list to force recalculation
            return Ok(Vec::new());
        }

        let persisted_columns = file_data
            .computed_columns
            .get(table_name)
            .cloned()
            .unwrap_or_default();

        // Convert back to ComputedColumn
        let computed_columns: Vec<ComputedColumn> = persisted_columns
            .into_iter()
            .map(|col| ComputedColumn {
                name: col.name,
                expression: col.expression,
                column_type: match col.column_type {
                    PersistedComputedColumnType::Aggregate(func) => ComputedColumnType::Aggregate(func),
                    PersistedComputedColumnType::RowOperation(cols) => ComputedColumnType::RowOperation(cols),
                    PersistedComputedColumnType::MixedOperation(cols, aggs) => ComputedColumnType::MixedOperation(cols, aggs),
                },
            })
            .collect();

        Ok(computed_columns)
    }

    pub fn should_recalculate(&self, file_path: &str) -> bool {
        match self.load_file_data(file_path) {
            Ok(file_data) => {
                match self.calculate_file_hash(file_path) {
                    Ok(current_hash) => current_hash != file_data.file_hash,
                    Err(_) => true, // If we can't calculate hash, assume recalculation needed
                }
            }
            Err(_) => false, // No saved data, no need to recalculate
        }
    }

    fn load_file_data(&self, file_path: &str) -> Result<FileComputedColumns> {
        let storage_file = self.get_storage_file_path(file_path);
        
        if !storage_file.exists() {
            return Err(anyhow::anyhow!("No saved computed columns for this file"));
        }

        let content = fs::read_to_string(&storage_file)
            .context("Failed to read computed columns file")?;
        let file_data: FileComputedColumns = serde_json::from_str(&content)
            .context("Failed to parse computed columns file")?;

        Ok(file_data)
    }

    fn get_storage_file_path(&self, file_path: &str) -> PathBuf {
        // Create a safe filename from the file path
        let safe_name = file_path
            .replace(['/', '\\', ':', '*', '?', '"', '<', '>', '|'], "_")
            .replace(' ', "_");
        
        self.storage_path.join(format!("{}.json", safe_name))
    }

    fn calculate_file_hash(&self, file_path: &str) -> Result<String> {
        let path = Path::new(file_path);
        if !path.exists() {
            return Err(anyhow::anyhow!("File not found: {}", file_path));
        }

        let metadata = fs::metadata(path)
            .context("Failed to read file metadata")?;
        
        // Simple hash based on file size and modification time
        let hash = format!(
            "{}_{}",
            metadata.len(),
            metadata
                .modified()
                .context("Failed to get file modification time")?
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_secs()
        );

        Ok(hash)
    }
}

fn get_storage_path() -> Result<PathBuf> {
    let home_dir = std::env::var("HOME")
        .context("HOME environment variable not set")?;
    let storage_dir = PathBuf::from(home_dir)
        .join(".local")
        .join("share")
        .join("sqbrowser");
    
    // Create storage directory if it doesn't exist
    if !storage_dir.exists() {
        fs::create_dir_all(&storage_dir)
            .context("Failed to create storage directory")?;
    }
    
    Ok(storage_dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_computed_column_persistence() {
        let temp_dir = tempdir().unwrap();
        let test_file = temp_dir.path().join("test.csv");
        fs::write(&test_file, "name,age\nJohn,25\nJane,30").unwrap();

        let persistence = ComputedColumnPersistence::new().unwrap();
        
        let computed_cols = vec![
            ComputedColumn {
                name: "age_doubled".to_string(),
                expression: "age * 2".to_string(),
                column_type: ComputedColumnType::RowOperation(vec!["age".to_string()]),
            }
        ];

        // Save computed columns
        persistence
            .save_computed_columns(
                test_file.to_str().unwrap(),
                "CSV Data",
                &computed_cols,
            )
            .unwrap();

        // Load computed columns
        let loaded_cols = persistence
            .load_computed_columns(test_file.to_str().unwrap(), "CSV Data")
            .unwrap();

        assert_eq!(loaded_cols.len(), 1);
        assert_eq!(loaded_cols[0].name, "age_doubled");
        assert_eq!(loaded_cols[0].expression, "age * 2");
    }
}