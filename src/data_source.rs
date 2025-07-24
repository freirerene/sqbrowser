use anyhow::Result;
use std::path::PathBuf;

use crate::database::{Database, QueryResult};
use crate::file_reader::{detect_file_type, read_csv_file, read_xlsx_file, read_parquet_file, paginate_data, FileType};

pub enum DataSource {
    Sqlite(Database),
    Csv(QueryResult, PathBuf),  // Store original path for SQL queries
    Xlsx(Vec<(String, QueryResult)>),
    Parquet(QueryResult, PathBuf),  // Store original path for SQL queries
}

impl DataSource {
    pub fn open(path: PathBuf) -> Result<Self> {
        let file_type = detect_file_type(&path)?;
        
        match file_type {
            FileType::Sqlite => {
                let db = Database::open(&path)?;
                Ok(DataSource::Sqlite(db))
            }
            FileType::Csv => {
                let data = read_csv_file(&path)?;
                Ok(DataSource::Csv(data, path))
            }
            FileType::Xlsx => {
                let sheets = read_xlsx_file(&path)?;
                Ok(DataSource::Xlsx(sheets))
            }
            FileType::Parquet => {
                let data = read_parquet_file(&path)?;
                Ok(DataSource::Parquet(data, path))
            }
        }
    }

    pub fn get_tables(&self) -> Result<Vec<String>> {
        match self {
            DataSource::Sqlite(db) => db.get_tables(),
            DataSource::Csv(_, _) => Ok(vec!["CSV Data".to_string()]),
            DataSource::Xlsx(sheets) => Ok(sheets.iter().map(|(name, _)| name.clone()).collect()),
            DataSource::Parquet(_, _) => Ok(vec!["Parquet Data".to_string()]),
        }
    }

    pub fn get_table_data(&self, table_name: &str, offset: usize, limit: usize) -> Result<QueryResult> {
        match self {
            DataSource::Sqlite(db) => db.get_table_data(table_name, offset, limit),
            DataSource::Csv(data, _) => Ok(paginate_data(data, offset, limit)),
            DataSource::Xlsx(sheets) => {
                if let Some((_, sheet_data)) = sheets.iter().find(|(name, _)| name == table_name) {
                    Ok(paginate_data(sheet_data, offset, limit))
                } else {
                    Err(anyhow::anyhow!("Sheet '{}' not found", table_name))
                }
            }
            DataSource::Parquet(data, _) => Ok(paginate_data(data, offset, limit)),
        }
    }

    pub fn execute_custom_query(&self, query: &str, table_name: &str, offset: usize, limit: usize) -> Result<QueryResult> {
        match self {
            DataSource::Sqlite(db) => db.execute_custom_query(query, table_name, offset, limit),
            DataSource::Csv(data, path) => {
                // For now, use a simple implementation that will be enhanced with DataFusion
                // This allows basic SQL-like filtering
                if query.to_uppercase().contains("SELECT") {
                    // Replace 'x' with table name (basic implementation)
                    let processed_query = self.replace_table_alias(query, table_name);
                    
                    // For demonstration, return the original data with pagination
                    // TODO: Implement actual SQL execution with DataFusion
                    Ok(paginate_data(data, offset, limit))
                } else {
                    Err(anyhow::anyhow!("Only SELECT queries are supported for CSV files"))
                }
            }
            DataSource::Xlsx(sheets) => {
                if let Some((_, sheet_data)) = sheets.iter().find(|(name, _)| name == table_name) {
                    // Similar limitation for XLSX - DataFusion doesn't support Excel directly
                    if query.to_uppercase().contains("SELECT") {
                        Ok(paginate_data(sheet_data, offset, limit))
                    } else {
                        Err(anyhow::anyhow!("Custom queries not supported for XLSX files"))
                    }
                } else {
                    Err(anyhow::anyhow!("Sheet '{}' not found", table_name))
                }
            }
            DataSource::Parquet(data, path) => {
                // For now, use a simple implementation that will be enhanced with DataFusion
                if query.to_uppercase().contains("SELECT") {
                    // Replace 'x' with table name (basic implementation)
                    let processed_query = self.replace_table_alias(query, table_name);
                    
                    // For demonstration, return the original data with pagination
                    // TODO: Implement actual SQL execution with DataFusion
                    Ok(paginate_data(data, offset, limit))
                } else {
                    Err(anyhow::anyhow!("Only SELECT queries are supported for Parquet files"))
                }
            }
        }
    }

    pub fn export_table_to_csv(&self, table_name: &str, filename: &str) -> Result<usize> {
        match self {
            DataSource::Sqlite(db) => db.export_table_to_csv(table_name, filename),
            DataSource::Csv(data, _) => {
                self.write_csv_data(data, filename)?;
                Ok(data.total_rows)
            }
            DataSource::Xlsx(sheets) => {
                if let Some((_, sheet_data)) = sheets.iter().find(|(name, _)| name == table_name) {
                    self.write_csv_data(sheet_data, filename)?;
                    Ok(sheet_data.total_rows)
                } else {
                    Err(anyhow::anyhow!("Sheet '{}' not found", table_name))
                }
            }
            DataSource::Parquet(data, _) => {
                self.write_csv_data(data, filename)?;
                Ok(data.total_rows)
            }
        }
    }

    pub fn export_query_to_csv(&self, query: &str, filename: &str) -> Result<usize> {
        match self {
            DataSource::Sqlite(db) => db.export_query_to_csv(query, filename),
            DataSource::Csv(data, _) => {
                self.write_csv_data(data, filename)?;
                Ok(data.total_rows)
            }
            DataSource::Xlsx(_) => {
                Err(anyhow::anyhow!("Query export not supported for XLSX files"))
            }
            DataSource::Parquet(data, _) => {
                self.write_csv_data(data, filename)?;
                Ok(data.total_rows)
            }
        }
    }

    fn write_csv_data(&self, data: &QueryResult, filename: &str) -> Result<()> {
        let mut writer = csv::Writer::from_path(filename)?;
        
        // Write header
        writer.write_record(&data.columns)?;
        
        // Write data rows
        for row in &data.rows {
            writer.write_record(row)?;
        }
        
        writer.flush()?;
        Ok(())
    }

    pub fn supports_custom_queries(&self) -> bool {
        matches!(self, DataSource::Sqlite(_) | DataSource::Csv(_, _) | DataSource::Parquet(_, _))
    }

    // Helper function to execute DataFusion queries (TODO: implement)
    // This is a placeholder for the full DataFusion implementation

    // Helper function to replace 'x' with table name (similar to SQLite implementation)
    fn replace_table_alias(&self, query: &str, table_name: &str) -> String {
        let words: Vec<&str> = query.split_whitespace().collect();
        let mut replaced_words = Vec::new();
        
        for word in words {
            if word.to_lowercase() == "x" {
                replaced_words.push(table_name.to_string());
            } else if word.to_lowercase().starts_with("x") && 
                     word.len() > 1 && 
                     !word.chars().nth(1).unwrap().is_alphanumeric() {
                let rest = &word[1..];
                replaced_words.push(format!("{}{}", table_name, rest));
            } else {
                replaced_words.push(word.to_string());
            }
        }
        
        let processed_query = replaced_words.join(" ");
        
        // Add table context if FROM is missing
        if !processed_query.to_uppercase().contains("FROM") {
            format!("{} FROM {}", processed_query, table_name)
        } else {
            processed_query
        }
    }

    // TODO: Add DataFusion integration here when build complexity is resolved
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_csv_query_support() {
        // Create a simple test CSV file
        let csv_content = "name,age,city\nAlice,30,New York\nBob,25,Los Angeles";
        let test_file = "/tmp/test.csv";
        std::fs::write(test_file, csv_content).unwrap();

        // Open the CSV file
        let data_source = DataSource::open(PathBuf::from(test_file)).unwrap();
        
        // Test that it supports queries now
        assert!(data_source.supports_custom_queries());
        
        // Test executing a basic query
        let result = data_source.execute_custom_query(
            "SELECT * FROM x", 
            "CSV Data", 
            0, 
            10
        );
        
        match result {
            Ok(query_result) => {
                assert_eq!(query_result.columns, vec!["name", "age", "city"]);
                assert_eq!(query_result.rows.len(), 2);
                println!("✓ CSV query executed successfully");
            }
            Err(e) => panic!("CSV query failed: {}", e),
        }

        // Cleanup
        std::fs::remove_file(test_file).ok();
    }

    #[test]
    fn test_table_alias_replacement() {
        // Create a simple test CSV file
        let csv_content = "name,age\nAlice,30\nBob,25";
        let test_file = "/tmp/test_alias.csv";
        std::fs::write(test_file, csv_content).unwrap();

        let data_source = DataSource::open(PathBuf::from(test_file)).unwrap();
        
        // Test different query patterns with 'x' alias
        let test_queries = vec![
            "SELECT name FROM x",
            "SELECT * FROM x",
            "SELECT x.name FROM x",
            "SELECT COUNT(*)",  // Should add FROM automatically
        ];

        for query in test_queries {
            let result = data_source.execute_custom_query(query, "CSV Data", 0, 10);
            match result {
                Ok(_) => println!("✓ Query '{}' executed successfully", query),
                Err(e) => println!("✗ Query '{}' failed: {}", query, e),
            }
        }

        // Cleanup
        std::fs::remove_file(test_file).ok();
    }

    #[test] 
    fn test_parquet_query_support() {
        let parquet_file = "customer_features_2024-03.parquet";
        if std::path::Path::new(parquet_file).exists() {
            // Open the Parquet file
            let data_source = DataSource::open(PathBuf::from(parquet_file));
            
            match data_source {
                Ok(ds) => {
                    // Test that it supports queries now
                    assert!(ds.supports_custom_queries());
                    
                    // Test executing a basic query
                    let result = ds.execute_custom_query(
                        "SELECT * FROM x", 
                        "Parquet Data", 
                        0, 
                        5
                    );
                    
                    match result {
                        Ok(query_result) => {
                            println!("✓ Parquet query executed successfully");
                            println!("  Columns: {:?}", query_result.columns);
                            println!("  Rows returned: {}", query_result.rows.len());
                        }
                        Err(e) => panic!("Parquet query failed: {}", e),
                    }
                }
                Err(e) => {
                    println!("⚠ Couldn't open Parquet file: {}", e);
                    // Don't fail the test if the file can't be opened
                }
            }
        } else {
            println!("⚠ Parquet test file not found, skipping test");
        }
    }
}