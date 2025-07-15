use anyhow::{Context, Result};
use rusqlite::{Connection, Row};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub total_rows: usize,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub total_rows: usize,
}

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path)
            .context("Failed to open database")?;
        Ok(Self { conn })
    }

    pub fn get_tables(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )?;
        
        let rows = stmt.query_map([], |row| {
            Ok(row.get::<_, String>(0)?)
        })?;

        let mut tables = Vec::new();
        for row in rows {
            tables.push(row?);
        }
        
        Ok(tables)
    }

    pub fn get_table_info(&self, table_name: &str) -> Result<TableInfo> {
        // Get column information
        let mut stmt = self.conn.prepare(&format!("PRAGMA table_info({})", table_name))?;
        let rows = stmt.query_map([], |row| {
            Ok(row.get::<_, String>(1)?) // Column name is at index 1
        })?;

        let mut columns = Vec::new();
        for row in rows {
            columns.push(row?);
        }

        // Get total row count
        let mut stmt = self.conn.prepare(&format!("SELECT COUNT(*) FROM {}", table_name))?;
        let total_rows: i64 = stmt.query_row([], |row| row.get(0))?;

        Ok(TableInfo {
            name: table_name.to_string(),
            columns,
            total_rows: total_rows as usize,
        })
    }

    pub fn get_table_data(
        &self,
        table_name: &str,
        offset: usize,
        limit: usize,
    ) -> Result<QueryResult> {
        let query = format!("SELECT * FROM {} LIMIT {} OFFSET {}", table_name, limit, offset);
        self.execute_query(&query)
    }

    pub fn execute_query(&self, query: &str) -> Result<QueryResult> {
        let mut stmt = self.conn.prepare(query)?;
        let column_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        
        let rows = stmt.query_map([], |row| {
            let mut values = Vec::new();
            for i in 0..column_names.len() {
                let value: rusqlite::types::Value = row.get(i)?;
                values.push(format_value(value));
            }
            Ok(values)
        })?;

        let mut result_rows = Vec::new();
        for row in rows {
            result_rows.push(row?);
        }

        // Try to get total count for the query (simplified approach)
        let total_rows = result_rows.len();

        Ok(QueryResult {
            columns: column_names,
            rows: result_rows,
            total_rows,
        })
    }

    pub fn execute_custom_query(
        &self,
        query: &str,
        table_name: &str,
        offset: usize,
        limit: usize,
    ) -> Result<QueryResult> {
        // Add table context if FROM is missing
        let final_query = if !query.to_uppercase().contains("FROM") {
            format!("{} FROM {}", query, table_name)
        } else {
            query.to_string()
        };

        // Add pagination
        let paginated_query = format!("{} LIMIT {} OFFSET {}", final_query, limit, offset);
        
        let mut stmt = self.conn.prepare(&paginated_query)?;
        let column_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        
        let rows = stmt.query_map([], |row| {
            let mut values = Vec::new();
            for i in 0..column_names.len() {
                let value: rusqlite::types::Value = row.get(i)?;
                values.push(format_value(value));
            }
            Ok(values)
        })?;

        let mut result_rows = Vec::new();
        for row in rows {
            result_rows.push(row?);
        }

        // Try to get total count for the custom query
        let count_query = format!("SELECT COUNT(*) FROM ({})", final_query);
        let total_rows = match self.conn.prepare(&count_query) {
            Ok(mut stmt) => {
                match stmt.query_row([], |row| row.get::<_, i64>(0)) {
                    Ok(count) => count as usize,
                    Err(_) => result_rows.len(), // Fallback to current result count
                }
            }
            Err(_) => result_rows.len(), // Fallback to current result count
        };

        Ok(QueryResult {
            columns: column_names,
            rows: result_rows,
            total_rows,
        })
    }

    pub fn export_table_to_csv(&self, table_name: &str, filename: &str) -> Result<usize> {
        let query = format!("SELECT * FROM {}", table_name);
        let result = self.execute_query(&query)?;
        self.write_csv(&result, filename)?;
        Ok(result.rows.len())
    }

    pub fn export_query_to_csv(&self, query: &str, filename: &str) -> Result<usize> {
        let result = self.execute_query(query)?;
        self.write_csv(&result, filename)?;
        Ok(result.rows.len())
    }

    fn write_csv(&self, result: &QueryResult, filename: &str) -> Result<()> {
        let mut writer = csv::Writer::from_path(filename)?;
        
        // Write header
        writer.write_record(&result.columns)?;
        
        // Write data rows
        for row in &result.rows {
            writer.write_record(row)?;
        }
        
        writer.flush()?;
        Ok(())
    }
}

fn format_value(value: rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".to_string(),
        rusqlite::types::Value::Integer(i) => i.to_string(),
        rusqlite::types::Value::Real(f) => f.to_string(),
        rusqlite::types::Value::Text(s) => s,
        rusqlite::types::Value::Blob(b) => format!("[BLOB {} bytes]", b.len()),
    }
}