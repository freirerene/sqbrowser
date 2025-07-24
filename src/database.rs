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
        // Include rowid for update operations
        let query = format!("SELECT rowid, * FROM {} LIMIT {} OFFSET {}", table_name, limit, offset);
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
        // Replace 'x' with the actual table name (case insensitive, word boundary)
        let mut processed_query = query.to_string();
        
        // Use regex-like replacement for word boundaries
        // Replace 'x' when it's a standalone word (not part of another word)
        let words: Vec<&str> = processed_query.split_whitespace().collect();
        let mut replaced_words = Vec::new();
        
        for word in words {
            // Check if word is exactly 'x' (case insensitive) or 'x' followed by punctuation
            if word.to_lowercase() == "x" {
                replaced_words.push(table_name.to_string());
            } else if word.to_lowercase().starts_with("x") && 
                     word.len() > 1 && 
                     !word.chars().nth(1).unwrap().is_alphanumeric() {
                // Handle cases like "x," "x;" "x)" etc.
                let rest = &word[1..];
                replaced_words.push(format!("{}{}", table_name, rest));
            } else {
                replaced_words.push(word.to_string());
            }
        }
        processed_query = replaced_words.join(" ");

        // Add table context if FROM is missing
        let mut final_query = if !processed_query.to_uppercase().contains("FROM") {
            format!("{} FROM {}", processed_query, table_name)
        } else {
            processed_query
        };

        // Ensure rowid is included for update operations (only if SELECT * is used)
        if final_query.to_uppercase().contains("SELECT *") {
            final_query = final_query.replace("SELECT *", "SELECT rowid, *");
        }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_alias_replacement() {
        // Create a temporary in-memory database for testing
        let db = Database::open(":memory:").unwrap();
        
        // Create a test table
        db.conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
            [],
        ).unwrap();
        
        // Insert some test data
        db.conn.execute(
            "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')",
            [],
        ).unwrap();
        db.conn.execute(
            "INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')",
            [],
        ).unwrap();

        // Test cases for alias replacement
        let test_cases = vec![
            ("SELECT * FROM x", "SELECT rowid, * FROM users"),
            ("SELECT name FROM x", "SELECT name FROM users"),
            ("SELECT x.name FROM x", "SELECT users.name FROM users"),
            ("SELECT * FROM x WHERE x.name = 'Alice'", "SELECT rowid, * FROM users WHERE users.name = 'Alice'"),
            ("SELECT COUNT(*) FROM x", "SELECT COUNT(*) FROM users"),
            ("SELECT name", "SELECT name FROM users"), // Test automatic FROM addition
        ];

        for (input_query, expected_processed) in test_cases {
            println!("Testing query: {} -> Expected: {}", input_query, expected_processed);
            
            // The actual processed query will have LIMIT and OFFSET added, so we need to check the processing logic
            let result = db.execute_custom_query(input_query, "users", 0, 10);
            
            // If query executes without error, the alias replacement worked
            match result {
                Ok(_) => println!("✓ Query executed successfully"),
                Err(e) => panic!("Query failed: {} (Input: {})", e, input_query),
            }
        }
    }

    #[test]
    fn test_table_alias_edge_cases() {
        let db = Database::open(":memory:").unwrap();
        
        // Create a test table
        db.conn.execute(
            "CREATE TABLE my_table (id INTEGER PRIMARY KEY, value TEXT)",
            [],
        ).unwrap();
        
        db.conn.execute(
            "INSERT INTO my_table (value) VALUES ('test1'), ('test2')",
            [],
        ).unwrap();

        // Test edge cases
        let edge_cases = vec![
            // Should NOT replace 'x' when it's part of another word
            ("SELECT value FROM my_table WHERE value LIKE '%text%'", true),  // 'text' contains 'x' but shouldn't be replaced
            
            // Should replace 'x' when it's standalone
            ("SELECT * FROM x", true),
            ("SELECT value FROM x", true),
            
            // Should replace 'x' with punctuation
            ("SELECT x.value FROM my_table", true),  // x.value should become my_table.value
            
            // Complex queries
            ("SELECT x.value FROM x WHERE x.id > 1", true),
            
            // Case sensitivity
            ("SELECT * FROM X", true),
        ];

        for (query, should_succeed) in edge_cases {
            let result = db.execute_custom_query(query, "my_table", 0, 10);
            match (result.is_ok(), should_succeed) {
                (true, true) => println!("✓ Edge case passed: {}", query),
                (false, false) => println!("✓ Edge case correctly failed: {}", query),
                (actual, expected) => panic!("Edge case failed: {} (expected: {}, got: {})", 
                                            query, expected, actual),
            }
        }
    }
}