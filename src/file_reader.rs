use anyhow::Result;
use calamine::{open_workbook, Data, Reader, Xlsx};
use csv::ReaderBuilder;
use std::path::Path;
use std::fs::File;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;

use crate::database::QueryResult;

#[derive(Debug, Clone, PartialEq)]
pub enum FileType {
    Sqlite,
    Csv,
    Xlsx,
    Parquet,
}

pub fn detect_file_type<P: AsRef<Path>>(path: P) -> Result<FileType> {
    let path = path.as_ref();
    let extension = path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        .to_lowercase();

    match extension.as_str() {
        "db" | "sqlite" | "sqlite3" => Ok(FileType::Sqlite),
        "csv" => Ok(FileType::Csv),
        "xlsx" | "xls" => Ok(FileType::Xlsx),
        "parquet" => Ok(FileType::Parquet),
        _ => {
            // Try to detect by content for files without clear extensions
            if is_sqlite_file(path)? {
                Ok(FileType::Sqlite)
            } else {
                // Default to CSV for text files
                Ok(FileType::Csv)
            }
        }
    }
}

fn is_sqlite_file<P: AsRef<Path>>(path: P) -> Result<bool> {
    use std::fs::File;
    use std::io::Read;

    let mut file = File::open(path)?;
    let mut buffer = [0; 16];
    let bytes_read = file.read(&mut buffer)?;
    
    if bytes_read >= 16 {
        // SQLite files start with "SQLite format 3\0"
        Ok(&buffer == b"SQLite format 3\0")
    } else {
        Ok(false)
    }
}

pub fn read_csv_file<P: AsRef<Path>>(path: P) -> Result<QueryResult> {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)?;

    let headers = reader.headers()?.clone();
    let columns: Vec<String> = headers.iter().map(|h| h.to_string()).collect();

    let mut rows = Vec::new();
    for result in reader.records() {
        let record = result?;
        let row: Vec<String> = record.iter().map(|field| field.to_string()).collect();
        rows.push(row);
    }

    let total_rows = rows.len();

    Ok(QueryResult {
        columns,
        rows,
        total_rows,
    })
}

pub fn read_xlsx_file<P: AsRef<Path>>(path: P) -> Result<Vec<(String, QueryResult)>> {
    let mut workbook: Xlsx<_> = open_workbook(path)?;
    let mut sheets = Vec::new();

    for sheet_name in workbook.sheet_names() {
        let sheet_name = sheet_name.to_string();
        
        if let Ok(range) = workbook.worksheet_range(&sheet_name) {
            let mut columns = Vec::new();
            let mut rows = Vec::new();

            // Get dimensions
            let (height, width) = range.get_size();
            
            if height == 0 || width == 0 {
                // Empty sheet
                sheets.push((sheet_name, QueryResult {
                    columns: vec!["Column1".to_string()],
                    rows: Vec::new(),
                    total_rows: 0,
                }));
                continue;
            }

            // Extract headers from first row
            for col in 0..width {
                let cell_value = range.get((0, col));
                let header = match cell_value {
                    Some(Data::String(s)) => s.clone(),
                    Some(Data::Float(f)) => f.to_string(),
                    Some(Data::Int(i)) => i.to_string(),
                    Some(Data::Bool(b)) => b.to_string(),
                    Some(Data::DateTime(dt)) => dt.to_string(),
                    Some(Data::DateTimeIso(dt)) => dt.clone(),
                    Some(Data::DurationIso(d)) => d.clone(),
                    Some(Data::Error(e)) => format!("Error: {:?}", e),
                    None | Some(Data::Empty) => format!("Column{}", col + 1),
                };
                columns.push(header);
            }

            // Extract data rows (skip header row)
            for row_idx in 1..height {
                let mut row_data = Vec::new();
                for col_idx in 0..width {
                    let cell_value = range.get((row_idx, col_idx));
                    let cell_string = match cell_value {
                        Some(Data::String(s)) => s.clone(),
                        Some(Data::Float(f)) => {
                            // Format floats nicely
                            if f.fract() == 0.0 {
                                format!("{:.0}", f)
                            } else {
                                f.to_string()
                            }
                        },
                        Some(Data::Int(i)) => i.to_string(),
                        Some(Data::Bool(b)) => b.to_string(),
                        Some(Data::DateTime(dt)) => dt.to_string(),
                        Some(Data::DateTimeIso(dt)) => dt.clone(),
                        Some(Data::DurationIso(d)) => d.clone(),
                        Some(Data::Error(e)) => format!("Error: {:?}", e),
                        None | Some(Data::Empty) => String::new(),
                    };
                    row_data.push(cell_string);
                }
                rows.push(row_data);
            }

            let total_rows = rows.len();
            sheets.push((sheet_name, QueryResult {
                columns,
                rows,
                total_rows,
            }));
        }
    }

    Ok(sheets)
}

pub fn read_parquet_file<P: AsRef<Path>>(path: P) -> Result<QueryResult> {
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();
    
    // Get column names from schema
    let schema = metadata.file_metadata().schema_descr();
    let mut columns = Vec::new();
    for i in 0..schema.num_columns() {
        let column = schema.column(i);
        columns.push(column.name().to_string());
    }
    
    // Read all row groups
    let mut rows = Vec::new();
    
    for row_group_idx in 0..metadata.num_row_groups() {
        let row_group_reader = reader.get_row_group(row_group_idx)?;
        let mut row_iter = row_group_reader.get_row_iter(None)?;
        
        while let Some(row_result) = row_iter.next() {
            let row = row_result?;
            let mut row_data = Vec::new();
            
            for col_idx in 0..columns.len() {
                let cell_value = match row.get_string(col_idx) {
                    Ok(val) => val.clone(),
                    Err(_) => {
                        // Try other types if string fails
                        match row.get_long(col_idx) {
                            Ok(val) => val.to_string(),
                            Err(_) => match row.get_double(col_idx) {
                                Ok(val) => val.to_string(),
                                Err(_) => match row.get_bool(col_idx) {
                                    Ok(val) => val.to_string(),
                                    Err(_) => "NULL".to_string(),
                                }
                            }
                        }
                    }
                };
                row_data.push(cell_value);
            }
            rows.push(row_data);
        }
    }
    
    let total_rows = rows.len();
    
    Ok(QueryResult {
        columns,
        rows,
        total_rows,
    })
}

pub fn paginate_data(data: &QueryResult, offset: usize, limit: usize) -> QueryResult {
    let end = (offset + limit).min(data.rows.len());
    let paginated_rows = if offset < data.rows.len() {
        data.rows[offset..end].to_vec()
    } else {
        Vec::new()
    };

    QueryResult {
        columns: data.columns.clone(),
        rows: paginated_rows,
        total_rows: data.total_rows,
    }
}