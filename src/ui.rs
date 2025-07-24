use anyhow::{Context, Result};
use arboard::Clipboard;
use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, TableState},
    Frame,
};

use crate::config::Theme;
use crate::data_source::DataSource;
use crate::database::QueryResult;
use crate::persistence::ComputedColumnPersistence;

#[derive(Debug, Clone, PartialEq)]
pub enum NavigationMode {
    Table,
    Data,
    Query,
    Edit,
    DetailedView,
    ErrorDisplay,
    ComputedColumn,
}

#[derive(Debug, Clone, PartialEq)]
enum MoveTo {
    Up,
    Down,
    Left,
    Right,
}

#[derive(Debug, Clone)]
pub struct ComputedColumn {
    pub name: String,
    pub expression: String,
    pub column_type: ComputedColumnType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ComputedColumnType {
    Aggregate(String),                        // sum, mean, count, etc.
    RowOperation(Vec<String>),                // operations on individual rows like Age + Height
    MixedOperation(Vec<String>, Vec<String>), // (columns, aggregate_expressions) like age*sum(height)
}

pub struct AppState {
    pub tables: Vec<String>,
    pub selected_table_idx: usize,
    pub selected_row_idx: usize,
    pub selected_col_idx: usize,
    pub navigation_mode: NavigationMode,
    pub current_query: Option<String>,
    pub query_input: String,
    pub data_offset: usize,
    pub page_size: usize,
    pub current_data: Option<QueryResult>,
    pub original_data: Option<QueryResult>, // Store original data for comparison
    pub db_path: String,
    pub status_message: Option<String>,
    pub show_help: bool,
    pub edit_input: String,
    pub editing_cell: Option<(usize, usize)>, // (row, col) indices
    pub data_modified: bool,
    pub detailed_view_row: Option<usize>, // Row index for detailed view
    pub detailed_view_selected_field: usize, // Selected field in detailed view
    pub clipboard: Option<Clipboard>,     // Persistent clipboard state
    pub error_message: Option<String>,    // Error message to display
    pub previous_navigation_mode: NavigationMode, // Previous mode before error display
    pub computed_column_input: String,    // Input for computed column expression
    pub computed_columns: Vec<ComputedColumn>, // List of computed columns
    pub persistence: ComputedColumnPersistence, // Persistence for computed columns
}

impl AppState {
    pub fn new(db_path: String, tables: Vec<String>) -> Result<Self> {
        let persistence = ComputedColumnPersistence::new()
            .context("Failed to initialize computed column persistence")?;

        Ok(Self {
            tables,
            selected_table_idx: 0,
            selected_row_idx: 0,
            selected_col_idx: 0,
            navigation_mode: NavigationMode::Table,
            current_query: None,
            query_input: String::new(),
            data_offset: 0,
            page_size: 25,
            current_data: None,
            original_data: None,
            db_path,
            status_message: None,
            show_help: false,
            edit_input: String::new(),
            editing_cell: None,
            data_modified: false,
            detailed_view_row: None,
            detailed_view_selected_field: 0,
            clipboard: None,
            error_message: None,
            previous_navigation_mode: NavigationMode::Data,
            computed_column_input: String::new(),
            computed_columns: Vec::new(),
            persistence,
        })
    }

    pub fn current_table(&self) -> Option<&str> {
        self.tables.get(self.selected_table_idx).map(|s| s.as_str())
    }

    pub fn handle_key_event(
        &mut self,
        key_event: KeyEvent,
        data_source: &DataSource,
    ) -> Result<bool> {
        // Handle help screen ESC in any mode
        if self.show_help && key_event.code == KeyCode::Esc {
            self.show_help = false;
            return Ok(true);
        }

        match self.navigation_mode {
            NavigationMode::Query => self.handle_query_input(key_event, data_source),
            NavigationMode::Table => self.handle_table_navigation(key_event, data_source),
            NavigationMode::Data => self.handle_data_navigation(key_event, data_source),
            NavigationMode::Edit => self.handle_edit_mode(key_event, data_source),
            NavigationMode::DetailedView => self.handle_detailed_view(key_event, data_source),
            NavigationMode::ErrorDisplay => self.handle_error_display(key_event, data_source),
            NavigationMode::ComputedColumn => {
                self.handle_computed_column_input(key_event, data_source)
            }
        }
    }

    fn handle_query_input(
        &mut self,
        key_event: KeyEvent,
        data_source: &DataSource,
    ) -> Result<bool> {
        match key_event.code {
            KeyCode::Esc => {
                self.navigation_mode = NavigationMode::Data;
                self.query_input.clear();
            }
            KeyCode::Enter => {
                if !self.query_input.trim().is_empty() {
                    if let Some(table_name) = self.current_table() {
                        if data_source.supports_custom_queries() {
                            match data_source.execute_custom_query(
                                &self.query_input,
                                table_name,
                                0,
                                self.page_size,
                            ) {
                                Ok(result) => {
                                    self.current_query = Some(self.query_input.clone());
                                    self.current_data = Some(result);
                                    self.selected_row_idx = 0;
                                    self.data_offset = 0;
                                    self.status_message =
                                        Some("Query executed successfully".to_string());
                                }
                                Err(e) => {
                                    self.show_error(format!("Query error: {}", e));
                                }
                            }
                        } else {
                            self.status_message =
                                Some("Custom queries not supported for this file type".to_string());
                        }
                    }
                }
                self.navigation_mode = NavigationMode::Data;
                self.query_input.clear();
            }
            KeyCode::Backspace => {
                self.query_input.pop();
            }
            KeyCode::Char(c) => {
                self.query_input.push(c);
            }
            _ => {}
        }
        Ok(true)
    }

    fn handle_table_navigation(
        &mut self,
        key_event: KeyEvent,
        data_source: &DataSource,
    ) -> Result<bool> {
        match key_event.code {
            KeyCode::Up => {
                if self.selected_table_idx > 0 {
                    self.selected_table_idx -= 1;
                    self.reset_data_view();
                    self.load_current_data(data_source)?;
                }
            }
            KeyCode::Down => {
                if self.selected_table_idx < self.tables.len().saturating_sub(1) {
                    self.selected_table_idx += 1;
                    self.reset_data_view();
                    self.load_current_data(data_source)?;
                }
            }
            KeyCode::Right | KeyCode::Enter => {
                self.navigation_mode = NavigationMode::Data;
                self.data_offset = 0;
                self.selected_row_idx = 0;
            }
            KeyCode::Char('q') | KeyCode::Char('c')
                if key_event.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                return Ok(false);
            }
            KeyCode::Char('h') => {
                self.show_help = !self.show_help;
            }
            _ => {}
        }
        Ok(true)
    }

    fn handle_data_navigation(
        &mut self,
        key_event: KeyEvent,
        data_source: &DataSource,
    ) -> Result<bool> {
        match key_event.code {
            KeyCode::Up => {
                if self.selected_row_idx > 0 {
                    self.selected_row_idx -= 1;
                } else if self.data_offset > 0 {
                    self.data_offset = self.data_offset.saturating_sub(self.page_size);
                    self.selected_row_idx = self.page_size - 1;
                    self.load_current_data(data_source)?;
                    if let Some(data) = &self.current_data {
                        if self.selected_row_idx >= data.rows.len() {
                            self.selected_row_idx = data.rows.len().saturating_sub(1);
                        }
                    }
                }
            }
            KeyCode::Down => {
                if let Some(data) = &self.current_data {
                    if self.selected_row_idx < data.rows.len().saturating_sub(1) {
                        self.selected_row_idx += 1;
                    } else if self.data_offset + data.rows.len() < data.total_rows {
                        self.data_offset += self.page_size;
                        self.selected_row_idx = 0;
                        self.load_current_data(data_source)?;
                    }
                }
            }
            KeyCode::Left => {
                if let Some(data) = &self.current_data {
                    let min_col = if !data.columns.is_empty() && data.columns[0] == "rowid" {
                        1
                    } else {
                        0
                    };
                    if self.selected_col_idx > min_col {
                        self.selected_col_idx -= 1;
                    } else {
                        // Go back to table view when at first column
                        self.navigation_mode = NavigationMode::Table;
                        self.reset_data_view();
                        self.load_current_data(data_source)?;
                    }
                } else {
                    self.navigation_mode = NavigationMode::Table;
                    self.reset_data_view();
                    self.load_current_data(data_source)?;
                }
            }
            KeyCode::Right => {
                if let Some(data) = &self.current_data {
                    if self.selected_col_idx < data.columns.len().saturating_sub(1) {
                        self.selected_col_idx += 1;
                    }
                }
            }
            KeyCode::PageUp => {
                if self.data_offset > 0 {
                    self.data_offset = self.data_offset.saturating_sub(self.page_size);
                    self.selected_row_idx = 0;
                    self.load_current_data(data_source)?;
                }
            }
            KeyCode::PageDown => {
                if let Some(data) = &self.current_data {
                    if self.data_offset + data.rows.len() < data.total_rows {
                        self.data_offset += self.page_size;
                        self.selected_row_idx = 0;
                        self.load_current_data(data_source)?;
                    }
                }
            }
            KeyCode::Home => {
                self.data_offset = 0;
                self.selected_row_idx = 0;
                self.load_current_data(data_source)?;
            }
            KeyCode::End => {
                if let Some(data) = &self.current_data {
                    self.data_offset = data.total_rows.saturating_sub(self.page_size);
                    self.selected_row_idx = 0;
                    self.load_current_data(data_source)?;
                }
            }
            KeyCode::Char(' ') => {
                if let Some(data) = &self.current_data {
                    if self.selected_row_idx < data.rows.len()
                        && self.selected_col_idx < data.columns.len()
                    {
                        // Prevent editing rowid column (column 0)
                        if !data.columns.is_empty()
                            && data.columns[0] == "rowid"
                            && self.selected_col_idx == 0
                        {
                            self.show_error("Cannot edit rowid column".to_string());
                            return Ok(true);
                        }

                        self.navigation_mode = NavigationMode::Edit;
                        self.editing_cell = Some((self.selected_row_idx, self.selected_col_idx));
                        self.edit_input =
                            data.rows[self.selected_row_idx][self.selected_col_idx].clone();
                    }
                }
            }
            KeyCode::Char('n') => {
                // Add new row
                if let Some(data) = &mut self.current_data {
                    let mut new_row: Vec<String> =
                        data.columns.iter().map(|_| String::new()).collect();
                    // Set rowid to empty for new rows (will be handled by INSERT)
                    if !data.columns.is_empty() && data.columns[0] == "rowid" {
                        new_row[0] = String::new();
                    }

                    data.rows.push(new_row);
                    data.total_rows += 1;
                    self.data_modified = true;
                    self.selected_row_idx = data.rows.len() - 1;
                    self.selected_col_idx = if data.columns.is_empty() || data.columns[0] != "rowid"
                    {
                        0
                    } else {
                        1
                    };
                    self.status_message = Some("New row added".to_string());
                }
            }
            KeyCode::Char('i') => {
                self.navigation_mode = NavigationMode::Query;
                self.query_input.clear();
            }
            KeyCode::Char('=') => {
                self.navigation_mode = NavigationMode::ComputedColumn;
                self.computed_column_input.clear();
            }
            KeyCode::Char('e') => {
                self.export_to_csv(data_source)?;
            }
            KeyCode::Char('s') => {
                // If we're in a custom query, warn user to go back to table view
                if self.current_query.is_some() {
                    self.show_error(
                        "Cannot save custom query results. Press 'r' to reload table data first."
                            .to_string(),
                    );
                } else {
                    self.save_changes(data_source)?;
                }
            }
            KeyCode::Char('r') => {
                // Clear custom query to reload original table data
                self.current_query = None;
                self.load_current_data(data_source)?;
            }
            KeyCode::Enter => {
                // Show detailed view for selected row
                if let Some(data) = &self.current_data {
                    if self.selected_row_idx < data.rows.len() {
                        self.detailed_view_row = Some(self.selected_row_idx);
                        self.detailed_view_selected_field = 0;
                        self.navigation_mode = NavigationMode::DetailedView;
                    }
                }
            }
            KeyCode::Char('q') | KeyCode::Char('c')
                if key_event.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                return Ok(false);
            }
            KeyCode::Char('h') => {
                self.show_help = !self.show_help;
            }
            _ => {}
        }
        Ok(true)
    }

    fn handle_edit_mode(&mut self, key_event: KeyEvent, data_source: &DataSource) -> Result<bool> {
        match key_event.code {
            KeyCode::Esc => {
                self.navigation_mode = NavigationMode::Data;
                self.editing_cell = None;
                self.edit_input.clear();
            }
            KeyCode::Enter => {
                if let Some((row_idx, col_idx)) = self.editing_cell {
                    if let Some(data) = &mut self.current_data {
                        if row_idx < data.rows.len() && col_idx < data.columns.len() {
                            // Don't allow saving changes to rowid column
                            if !data.columns.is_empty()
                                && data.columns[0] == "rowid"
                                && col_idx == 0
                            {
                                self.show_error("Cannot edit rowid column".to_string());
                            } else {
                                data.rows[row_idx][col_idx] = self.edit_input.clone();
                                self.data_modified = true;
                                self.status_message = Some("Cell updated (not saved)".to_string());
                            }
                        }
                    }
                }
                self.navigation_mode = NavigationMode::Data;
                self.editing_cell = None;
                self.edit_input.clear();

                // Refresh computed columns after edit
                if let Err(e) = self.refresh_computed_columns() {
                    self.show_error(format!("Failed to update computed columns: {}", e));
                }
            }
            KeyCode::Up => {
                self.save_current_edit_and_move_to(MoveTo::Up, data_source)?;
            }
            KeyCode::Down => {
                self.save_current_edit_and_move_to(MoveTo::Down, data_source)?;
            }
            KeyCode::Left => {
                self.save_current_edit_and_move_to(MoveTo::Left, data_source)?;
            }
            KeyCode::Right => {
                self.save_current_edit_and_move_to(MoveTo::Right, data_source)?;
            }
            KeyCode::Backspace => {
                self.edit_input.pop();
            }
            KeyCode::Char('n') if key_event.modifiers.contains(KeyModifiers::CONTROL) => {
                // Add new row
                if let Some(data) = &mut self.current_data {
                    let mut new_row: Vec<String> =
                        data.columns.iter().map(|_| String::new()).collect();
                    // Set rowid to empty for new rows (will be handled by INSERT)
                    if !data.columns.is_empty() && data.columns[0] == "rowid" {
                        new_row[0] = String::new();
                    }

                    data.rows.push(new_row);
                    data.total_rows += 1;
                    self.data_modified = true;
                    self.selected_row_idx = data.rows.len() - 1;
                    self.selected_col_idx = if data.columns.is_empty() || data.columns[0] != "rowid"
                    {
                        0
                    } else {
                        1
                    };
                    self.editing_cell = Some((self.selected_row_idx, self.selected_col_idx));
                    self.edit_input.clear();
                    self.status_message = Some("New row added".to_string());
                }
            }
            KeyCode::Char(c) => {
                self.edit_input.push(c);
            }
            KeyCode::Tab => {
                // Save current edit and move to next cell
                if let Some((row_idx, col_idx)) = self.editing_cell {
                    if let Some(data) = &mut self.current_data {
                        if row_idx < data.rows.len() && col_idx < data.columns.len() {
                            // Don't allow saving changes to rowid column
                            if !data.columns.is_empty()
                                && data.columns[0] == "rowid"
                                && col_idx == 0
                            {
                                // Skip saving changes to rowid column
                            } else {
                                data.rows[row_idx][col_idx] = self.edit_input.clone();
                                self.data_modified = true;
                            }

                            // Move to next cell
                            if col_idx < data.columns.len() - 1 {
                                self.selected_col_idx += 1;
                                self.editing_cell = Some((row_idx, col_idx + 1));
                                self.edit_input = data.rows[row_idx][col_idx + 1].clone();
                            } else if row_idx < data.rows.len() - 1 {
                                self.selected_row_idx += 1;
                                let min_col =
                                    if !data.columns.is_empty() && data.columns[0] == "rowid" {
                                        1
                                    } else {
                                        0
                                    };
                                self.selected_col_idx = min_col;
                                self.editing_cell = Some((row_idx + 1, min_col));
                                self.edit_input = data.rows[row_idx + 1][min_col].clone();
                            } else {
                                // At the end, exit edit mode
                                self.navigation_mode = NavigationMode::Data;
                                self.editing_cell = None;
                                self.edit_input.clear();
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(true)
    }

    fn save_current_edit_and_move_to(
        &mut self,
        direction: MoveTo,
        data_source: &DataSource,
    ) -> Result<()> {
        // Save current edit
        if let Some((row_idx, col_idx)) = self.editing_cell {
            if let Some(data) = &mut self.current_data {
                if row_idx < data.rows.len() && col_idx < data.columns.len() {
                    // Don't allow saving changes to rowid column
                    if !data.columns.is_empty() && data.columns[0] == "rowid" && col_idx == 0 {
                        // Skip saving changes to rowid column
                    } else {
                        data.rows[row_idx][col_idx] = self.edit_input.clone();
                        self.data_modified = true;
                    }
                }
            }
        }

        // Move to new position
        if let Some(data) = &self.current_data {
            let (mut new_row, mut new_col) = (self.selected_row_idx, self.selected_col_idx);

            match direction {
                MoveTo::Up => {
                    if new_row > 0 {
                        new_row -= 1;
                    } else if self.data_offset > 0 {
                        self.data_offset = self.data_offset.saturating_sub(self.page_size);
                        new_row = self.page_size - 1;
                        self.load_current_data(data_source)?;
                        if let Some(data) = &self.current_data {
                            if new_row >= data.rows.len() {
                                new_row = data.rows.len().saturating_sub(1);
                            }
                        }
                    }
                }
                MoveTo::Down => {
                    if new_row < data.rows.len().saturating_sub(1) {
                        new_row += 1;
                    } else if self.data_offset + data.rows.len() < data.total_rows {
                        self.data_offset += self.page_size;
                        new_row = 0;
                        self.load_current_data(data_source)?;
                    }
                }
                MoveTo::Left => {
                    let min_col = if !data.columns.is_empty() && data.columns[0] == "rowid" {
                        1
                    } else {
                        0
                    };
                    if new_col > min_col {
                        new_col -= 1;
                    }
                }
                MoveTo::Right => {
                    if new_col < data.columns.len().saturating_sub(1) {
                        new_col += 1;
                    }
                }
            }

            // Update position and edit input
            self.selected_row_idx = new_row;
            self.selected_col_idx = new_col;
            self.editing_cell = Some((new_row, new_col));

            // Load new cell content
            if let Some(data) = &self.current_data {
                if new_row < data.rows.len() && new_col < data.columns.len() {
                    self.edit_input = data.rows[new_row][new_col].clone();
                }
            }
        }

        Ok(())
    }

    fn reset_data_view(&mut self) {
        self.current_query = None;
        self.current_data = None;
        self.original_data = None;
        self.selected_row_idx = 0;
        self.selected_col_idx = 0;
        self.data_offset = 0;
        self.editing_cell = None;
        self.edit_input.clear();
        self.data_modified = false;
    }

    fn ensure_valid_col_selection(&mut self) {
        if let Some(data) = &self.current_data {
            let min_col = if !data.columns.is_empty() && data.columns[0] == "rowid" {
                1
            } else {
                0
            };
            if self.selected_col_idx < min_col {
                self.selected_col_idx = min_col;
            }
        }
    }

    pub fn load_current_data(&mut self, data_source: &DataSource) -> Result<()> {
        if let Some(table_name) = self.current_table().map(|s| s.to_string()) {
            let result = if let Some(query) = &self.current_query {
                data_source.execute_custom_query(
                    query,
                    &table_name,
                    self.data_offset,
                    self.page_size,
                )?
            } else {
                data_source.get_table_data(&table_name, self.data_offset, self.page_size)?
            };

            // Store original data for comparison when saving
            self.original_data = Some(result.clone());
            self.current_data = Some(result);

            // Load saved computed columns if available
            self.load_computed_columns(&table_name)?;

            // Apply computed columns to the loaded data
            self.apply_computed_columns(data_source)?;

            // Ensure column selection is valid (skip rowid)
            self.ensure_valid_col_selection();
        }
        Ok(())
    }

    fn load_computed_columns(&mut self, table_name: &str) -> Result<()> {
        // Check if file has changed and recalculation is needed
        if self.persistence.should_recalculate(&self.db_path) {
            // File has changed, clear computed columns to force user to recreate them
            // This is a safety measure to prevent incorrect calculations
            self.computed_columns.clear();
            return Ok(());
        }

        match self
            .persistence
            .load_computed_columns(&self.db_path, table_name)
        {
            Ok(columns) => {
                self.computed_columns = columns;
            }
            Err(_) => {
                // No saved columns or file doesn't exist, start with empty list
                self.computed_columns.clear();
            }
        }
        Ok(())
    }

    fn save_computed_columns(&self, table_name: &str) -> Result<()> {
        self.persistence
            .save_computed_columns(&self.db_path, table_name, &self.computed_columns)
            .context("Failed to save computed columns")?;
        Ok(())
    }

    fn export_to_csv(&mut self, data_source: &DataSource) -> Result<()> {
        if let Some(table_name) = self.current_table() {
            let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
            let filename = if let Some(_query) = &self.current_query {
                format!("query_export_{}.csv", timestamp)
            } else {
                format!("{}_{}.csv", table_name, timestamp)
            };

            let rows_exported = if let Some(query) = &self.current_query {
                data_source.export_query_to_csv(query, &filename)?
            } else {
                data_source.export_table_to_csv(table_name, &filename)?
            };

            self.status_message = Some(format!("Exported {} rows to {}", rows_exported, filename));
        }
        Ok(())
    }

    pub fn save_changes(&mut self, data_source: &DataSource) -> Result<()> {
        if !self.data_modified {
            self.status_message = Some("No changes to save".to_string());
            return Ok(());
        }

        let table_name = self.current_table().map(|s| s.to_string());
        if let Some(table_name) = table_name {
            if let Some(data) = self.current_data.clone() {
                // For now, we'll only support saving to CSV files
                // SQLite and Excel would need more complex update logic
                match data_source {
                    crate::data_source::DataSource::Csv(_, _) => {
                        let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
                        let filename = format!("{}_edited_{}.csv", table_name, timestamp);
                        self.write_csv_data(&data, &filename)?;
                        self.data_modified = false;
                        self.status_message = Some(format!("Changes saved to {}", filename));
                    }
                    crate::data_source::DataSource::Xlsx(_) => {
                        let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
                        let filename = format!("{}_edited_{}.csv", table_name, timestamp);
                        self.write_csv_data(&data, &filename)?;
                        self.data_modified = false;
                        self.status_message = Some(format!(
                            "Changes saved to {} (converted from Excel)",
                            filename
                        ));
                    }
                    crate::data_source::DataSource::Sqlite(_) => {
                        let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
                        let filename = format!("{}_edited_{}.csv", table_name, timestamp);
                        self.write_csv_data(&data, &filename)?;
                        self.data_modified = false;
                        self.status_message = Some(format!(
                            "Changes exported to {} (SQLite direct save not supported)",
                            filename
                        ));
                    }
                    crate::data_source::DataSource::Parquet(_, _) => {
                        let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
                        let filename = format!("{}_edited_{}.csv", table_name, timestamp);
                        self.write_csv_data(&data, &filename)?;
                        self.data_modified = false;
                        self.status_message = Some(format!(
                            "Changes saved to {} (converted from Parquet)",
                            filename
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn write_csv_data(&self, data: &crate::database::QueryResult, filename: &str) -> Result<()> {
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

    fn handle_detailed_view(
        &mut self,
        key_event: KeyEvent,
        _data_source: &DataSource,
    ) -> Result<bool> {
        match key_event.code {
            KeyCode::Esc => {
                self.navigation_mode = NavigationMode::Data;
                self.detailed_view_row = None;
                self.detailed_view_selected_field = 0;
            }
            KeyCode::Up => {
                if let Some(data) = &self.current_data {
                    if self.detailed_view_selected_field > 0 {
                        self.detailed_view_selected_field -= 1;
                    }
                }
            }
            KeyCode::Down => {
                if let Some(data) = &self.current_data {
                    if self.detailed_view_selected_field < data.columns.len().saturating_sub(1) {
                        self.detailed_view_selected_field += 1;
                    }
                }
            }
            KeyCode::Char('c') if !key_event.modifiers.contains(KeyModifiers::CONTROL) => {
                // Copy selected field value to clipboard
                if let Some(row_idx) = self.detailed_view_row {
                    if let Some(data) = &self.current_data {
                        if row_idx < data.rows.len()
                            && self.detailed_view_selected_field < data.columns.len()
                        {
                            let value =
                                data.rows[row_idx][self.detailed_view_selected_field].clone();
                            match self.copy_to_clipboard(&value) {
                                Ok(_) => {
                                    self.status_message = Some("Copied to clipboard".to_string());
                                }
                                Err(e) => {
                                    self.show_error(format!("Failed to copy to clipboard: {}", e));
                                }
                            }
                        }
                    }
                }
            }
            KeyCode::Char('q') | KeyCode::Char('c')
                if key_event.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                return Ok(false);
            }
            _ => {}
        }
        Ok(true)
    }

    fn copy_to_clipboard(&mut self, text: &str) -> Result<()> {
        if self.clipboard.is_none() {
            self.clipboard = Some(Clipboard::new()?);
        }

        if let Some(clipboard) = &mut self.clipboard {
            clipboard.set_text(text)?;
            // Small delay to ensure clipboard managers have time to see the content
            std::thread::sleep(std::time::Duration::from_millis(150));
        }
        Ok(())
    }

    fn show_error(&mut self, error: String) {
        self.error_message = Some(error);
        self.previous_navigation_mode = self.navigation_mode.clone();
        self.navigation_mode = NavigationMode::ErrorDisplay;
    }

    fn handle_error_display(
        &mut self,
        key_event: KeyEvent,
        _data_source: &DataSource,
    ) -> Result<bool> {
        match key_event.code {
            KeyCode::Esc => {
                self.navigation_mode = self.previous_navigation_mode.clone();
                self.error_message = None;
            }
            KeyCode::Char('q') | KeyCode::Char('c')
                if key_event.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                return Ok(false);
            }
            _ => {}
        }
        Ok(true)
    }

    fn handle_computed_column_input(
        &mut self,
        key_event: KeyEvent,
        data_source: &DataSource,
    ) -> Result<bool> {
        match key_event.code {
            KeyCode::Esc => {
                self.navigation_mode = NavigationMode::Data;
                self.computed_column_input.clear();
            }
            KeyCode::Enter => {
                if !self.computed_column_input.trim().is_empty() {
                    match self.parse_and_add_computed_column(&self.computed_column_input.clone()) {
                        Ok(_) => {
                            self.apply_computed_columns(data_source)?;
                            // Save computed columns to persistence
                            if let Some(table_name) = self.current_table() {
                                if let Err(e) = self.save_computed_columns(table_name) {
                                    self.status_message =
                                        Some(format!("Column added but save failed: {}", e));
                                } else {
                                    self.status_message =
                                        Some("Computed column added and saved".to_string());
                                }
                            } else {
                                self.status_message = Some("Computed column added".to_string());
                            }
                        }
                        Err(e) => {
                            self.show_error(format!("Expression error: {}", e));
                        }
                    }
                }
                self.navigation_mode = NavigationMode::Data;
                self.computed_column_input.clear();
            }
            KeyCode::Backspace => {
                self.computed_column_input.pop();
            }
            KeyCode::Char(c) => {
                self.computed_column_input.push(c);
            }
            _ => {}
        }
        Ok(true)
    }

    fn parse_and_add_computed_column(&mut self, expression: &str) -> Result<()> {
        let expression = expression.trim();

        // Check if expression has custom name (contains '=')
        let (column_name, expr_part) = if let Some(eq_pos) = expression.find('=') {
            let name = expression[..eq_pos].trim();
            let expr = expression[eq_pos + 1..].trim();
            if name.is_empty() || expr.is_empty() {
                return Err(anyhow::anyhow!(
                    "Invalid syntax. Use 'column_name=expression'"
                ));
            }
            // Validate column name (no special characters except underscore)
            if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
                return Err(anyhow::anyhow!(
                    "Column name can only contain letters, numbers, and underscores"
                ));
            }
            (Some(name.to_string()), expr)
        } else {
            (None, expression)
        };

        // Parse different types of expressions
        if let Some(captures) = regex::Regex::new(r"^(sum|mean|count|min|max)\(([^)]+)\)$")
            .unwrap()
            .captures(expr_part)
        {
            // Aggregate function
            let func = captures.get(1).unwrap().as_str();
            let column = captures.get(2).unwrap().as_str().trim();

            // Verify column exists
            if let Some(data) = &self.current_data {
                if !data.columns.contains(&column.to_string()) {
                    return Err(anyhow::anyhow!("Column '{}' does not exist", column));
                }
            }

            let computed_col = ComputedColumn {
                name: column_name.unwrap_or_else(|| format!("{}({})", func, column)),
                expression: expr_part.to_string(),
                column_type: ComputedColumnType::Aggregate(func.to_string()),
            };

            self.computed_columns.push(computed_col);
            Ok(())
        } else if expr_part.contains('+')
            || expr_part.contains('-')
            || expr_part.contains('*')
            || expr_part.contains('/')
            || expr_part
                .chars()
                .all(|c| c.is_ascii_digit() || c == '.' || c == ' ')
        {
            // Row operation, mixed operation, or constant expression
            let columns_used = self.extract_column_names(expr_part)?;
            let aggregate_expressions = self.extract_aggregate_expressions(expr_part)?;

            // Verify all columns exist if any are used
            if let Some(data) = &self.current_data {
                for col in &columns_used {
                    if !data.columns.contains(col) {
                        return Err(anyhow::anyhow!("Column '{}' does not exist", col));
                    }
                }
                // Verify columns in aggregate expressions exist
                for agg_expr in &aggregate_expressions {
                    let column_in_agg = self.extract_column_from_aggregate(agg_expr)?;
                    if !data.columns.contains(&column_in_agg) {
                        return Err(anyhow::anyhow!(
                            "Column '{}' in aggregate '{}' does not exist",
                            column_in_agg,
                            agg_expr
                        ));
                    }
                }
            }

            let column_type = if aggregate_expressions.is_empty() {
                ComputedColumnType::RowOperation(columns_used)
            } else {
                ComputedColumnType::MixedOperation(columns_used, aggregate_expressions)
            };

            let computed_col = ComputedColumn {
                name: column_name.unwrap_or_else(|| expr_part.to_string()),
                expression: expr_part.to_string(),
                column_type,
            };

            self.computed_columns.push(computed_col);
            Ok(())
        } else {
            // Check if it's a simple numeric constant or column name
            if expr_part.trim().parse::<f64>().is_ok() {
                // It's a numeric constant
                let computed_col = ComputedColumn {
                    name: column_name.unwrap_or_else(|| expr_part.to_string()),
                    expression: expr_part.to_string(),
                    column_type: ComputedColumnType::RowOperation(vec![]),
                };

                self.computed_columns.push(computed_col);
                Ok(())
            } else if let Some(data) = &self.current_data {
                // Check if it's a column name
                if data.columns.contains(&expr_part.to_string()) {
                    let computed_col = ComputedColumn {
                        name: column_name.unwrap_or_else(|| expr_part.to_string()),
                        expression: expr_part.to_string(),
                        column_type: ComputedColumnType::RowOperation(vec![expr_part.to_string()]),
                    };

                    self.computed_columns.push(computed_col);
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("Invalid expression format. Use sum(Column), mean(Column), Column1 + Column2, or numeric constants"))
                }
            } else {
                Err(anyhow::anyhow!("Invalid expression format. Use sum(Column), mean(Column), Column1 + Column2, or numeric constants"))
            }
        }
    }

    fn extract_column_names(&self, expression: &str) -> Result<Vec<String>> {
        let mut columns = Vec::new();
        let mut current_token = String::new();
        let mut in_column = false;

        for ch in expression.chars() {
            match ch {
                '+' | '-' | '*' | '/' | '(' | ')' | ' ' | ',' => {
                    if in_column && !current_token.trim().is_empty() {
                        let token = current_token.trim().to_string();
                        // Only add if it's not a number and not a function name
                        if !token.parse::<f64>().is_ok()
                            && !["sum", "mean", "count", "min", "max"].contains(&token.as_str())
                        {
                            columns.push(token);
                        }
                        current_token.clear();
                        in_column = false;
                    }
                }
                _ => {
                    if !in_column && !ch.is_whitespace() {
                        in_column = true;
                    }
                    if in_column {
                        current_token.push(ch);
                    }
                }
            }
        }

        if in_column && !current_token.trim().is_empty() {
            let token = current_token.trim().to_string();
            if !token.parse::<f64>().is_ok()
                && !["sum", "mean", "count", "min", "max"].contains(&token.as_str())
            {
                columns.push(token);
            }
        }

        // Remove duplicates
        columns.sort();
        columns.dedup();

        Ok(columns)
    }

    fn extract_aggregate_expressions(&self, expression: &str) -> Result<Vec<String>> {
        let mut aggregates = Vec::new();
        let regex = regex::Regex::new(r"(sum|mean|count|min|max)\([^)]+\)").unwrap();

        for capture in regex.captures_iter(expression) {
            if let Some(full_match) = capture.get(0) {
                aggregates.push(full_match.as_str().to_string());
            }
        }

        Ok(aggregates)
    }

    fn extract_column_from_aggregate(&self, aggregate_expr: &str) -> Result<String> {
        let regex = regex::Regex::new(r"^(sum|mean|count|min|max)\(([^)]+)\)$").unwrap();

        if let Some(captures) = regex.captures(aggregate_expr) {
            if let Some(column_match) = captures.get(2) {
                return Ok(column_match.as_str().trim().to_string());
            }
        }

        Err(anyhow::anyhow!(
            "Invalid aggregate expression: {}",
            aggregate_expr
        ))
    }

    fn apply_computed_columns(&mut self, _data_source: &DataSource) -> Result<()> {
        if let Some(data) = &mut self.current_data {
            for computed_col in &self.computed_columns {
                // Check if column already exists, if so, remove it first
                if let Some(pos) = data.columns.iter().position(|x| x == &computed_col.name) {
                    data.columns.remove(pos);
                    for row in &mut data.rows {
                        if pos < row.len() {
                            row.remove(pos);
                        }
                    }
                }

                // Add the new computed column
                data.columns.push(computed_col.name.clone());

                match &computed_col.column_type {
                    ComputedColumnType::Aggregate(func) => {
                        let value =
                            Self::compute_aggregate_static(data, func, &computed_col.expression)?;
                        for row in &mut data.rows {
                            row.push(value.clone());
                        }
                    }
                    ComputedColumnType::RowOperation(columns_used) => {
                        let expression = computed_col.expression.clone();
                        let cols = columns_used.clone();
                        let mut computed_values = Vec::new();

                        for row in &data.rows {
                            let value =
                                Self::compute_row_operation_static(data, row, &expression, &cols)?;
                            computed_values.push(value);
                        }

                        for (row, value) in data.rows.iter_mut().zip(computed_values) {
                            row.push(value);
                        }
                    }
                    ComputedColumnType::MixedOperation(columns_used, aggregate_expressions) => {
                        let expression = computed_col.expression.clone();
                        let cols = columns_used.clone();
                        let aggs = aggregate_expressions.clone();
                        let mut computed_values = Vec::new();

                        for row in &data.rows {
                            let value = Self::compute_mixed_operation_static(
                                data,
                                row,
                                &expression,
                                &cols,
                                &aggs,
                            )?;
                            computed_values.push(value);
                        }

                        for (row, value) in data.rows.iter_mut().zip(computed_values) {
                            row.push(value);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn compute_aggregate_static(
        data: &QueryResult,
        func: &str,
        expression: &str,
    ) -> Result<String> {
        // Extract column name from expression like "sum(Age)"
        let column_name = expression
            .trim_start_matches(func)
            .trim_start_matches('(')
            .trim_end_matches(')')
            .trim();

        let col_idx = data
            .columns
            .iter()
            .position(|col| col == column_name)
            .ok_or_else(|| anyhow::anyhow!("Column '{}' not found", column_name))?;

        let mut values = Vec::new();
        for row in &data.rows {
            if col_idx < row.len() {
                if let Ok(val) = row[col_idx].parse::<f64>() {
                    values.push(val);
                }
            }
        }

        if values.is_empty() {
            return Ok("0".to_string());
        }

        let result = match func {
            "sum" => values.iter().sum::<f64>(),
            "mean" => values.iter().sum::<f64>() / values.len() as f64,
            "count" => values.len() as f64,
            "min" => values.iter().fold(f64::INFINITY, |a, &b| a.min(b)),
            "max" => values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b)),
            _ => return Err(anyhow::anyhow!("Unknown function: {}", func)),
        };

        Ok(if result.fract() == 0.0 {
            format!("{:.0}", result)
        } else {
            format!("{:.2}", result)
        })
    }

    fn compute_row_operation_static(
        data: &QueryResult,
        row: &[String],
        expression: &str,
        columns_used: &[String],
    ) -> Result<String> {
        let mut expr = expression.to_string();

        // Replace column names with their values
        for col_name in columns_used {
            if let Some(col_idx) = data.columns.iter().position(|col| col == col_name) {
                if col_idx < row.len() {
                    let value = row[col_idx].parse::<f64>().unwrap_or(0.0);
                    expr = expr.replace(col_name, &value.to_string());
                }
            }
        }

        // Simple expression evaluator for basic math operations
        Self::evaluate_expression_static(&expr)
    }

    fn compute_mixed_operation_static(
        data: &QueryResult,
        row: &[String],
        expression: &str,
        columns_used: &[String],
        aggregate_expressions: &[String],
    ) -> Result<String> {
        let mut expr = expression.to_string();

        // First, replace aggregate expressions with their computed values
        for agg_expr in aggregate_expressions {
            // Parse the aggregate function and column
            let regex = regex::Regex::new(r"^(sum|mean|count|min|max)\(([^)]+)\)$").unwrap();
            if let Some(captures) = regex.captures(agg_expr) {
                let func = captures.get(1).unwrap().as_str();
                let agg_value = Self::compute_aggregate_static(data, func, agg_expr)?;
                expr = expr.replace(agg_expr, &agg_value);
            }
        }

        // Then, replace column names with their values from the current row
        for col_name in columns_used {
            if let Some(col_idx) = data.columns.iter().position(|col| col == col_name) {
                if col_idx < row.len() {
                    let value = row[col_idx].parse::<f64>().unwrap_or(0.0);
                    expr = expr.replace(col_name, &value.to_string());
                }
            }
        }

        // Finally, evaluate the expression
        Self::evaluate_expression_static(&expr)
    }

    fn evaluate_expression_static(expr: &str) -> Result<String> {
        // Simple evaluator for basic arithmetic with proper operator precedence
        let expr = expr.replace(" ", "");

        // Handle parentheses first
        if let Some(start) = expr.rfind('(') {
            if let Some(end) = expr[start..].find(')') {
                let inner = &expr[start + 1..start + end];
                let inner_result = Self::evaluate_expression_static(inner)?;
                let new_expr = format!(
                    "{}{}{}",
                    &expr[..start],
                    inner_result,
                    &expr[start + end + 1..]
                );
                return Self::evaluate_expression_static(&new_expr);
            }
        }

        // Handle multiplication/division (higher precedence)
        if let Some(pos) = expr.rfind('*') {
            let left = Self::evaluate_expression_static(&expr[..pos])?;
            let right = Self::evaluate_expression_static(&expr[pos + 1..])?;
            let result = left.parse::<f64>()? * right.parse::<f64>()?;
            return Ok(if result.fract() == 0.0 {
                format!("{:.0}", result)
            } else {
                format!("{:.2}", result)
            });
        }

        if let Some(pos) = expr.rfind('/') {
            let left = Self::evaluate_expression_static(&expr[..pos])?;
            let right = Self::evaluate_expression_static(&expr[pos + 1..])?;
            let right_val = right.parse::<f64>()?;
            if right_val == 0.0 {
                return Err(anyhow::anyhow!("Division by zero"));
            }
            let result = left.parse::<f64>()? / right_val;
            return Ok(if result.fract() == 0.0 {
                format!("{:.0}", result)
            } else {
                format!("{:.2}", result)
            });
        }

        // Handle addition/subtraction (lower precedence)
        if let Some(pos) = expr.rfind('+') {
            let left = Self::evaluate_expression_static(&expr[..pos])?;
            let right = Self::evaluate_expression_static(&expr[pos + 1..])?;
            let result = left.parse::<f64>()? + right.parse::<f64>()?;
            return Ok(if result.fract() == 0.0 {
                format!("{:.0}", result)
            } else {
                format!("{:.2}", result)
            });
        }

        if let Some(pos) = expr.rfind('-') {
            // Make sure this isn't a negative number at the start
            if pos > 0 {
                let left = Self::evaluate_expression_static(&expr[..pos])?;
                let right = Self::evaluate_expression_static(&expr[pos + 1..])?;
                let result = left.parse::<f64>()? - right.parse::<f64>()?;
                return Ok(if result.fract() == 0.0 {
                    format!("{:.0}", result)
                } else {
                    format!("{:.2}", result)
                });
            }
        }

        // Base case - just a number
        if let Ok(num) = expr.parse::<f64>() {
            Ok(if num.fract() == 0.0 {
                format!("{:.0}", num)
            } else {
                format!("{:.2}", num)
            })
        } else {
            Ok(expr.to_string())
        }
    }

    fn refresh_computed_columns(&mut self) -> Result<()> {
        if let Some(data) = &mut self.current_data {
            // Remove all computed columns first
            let mut cols_to_remove = Vec::new();
            for computed_col in &self.computed_columns {
                if let Some(pos) = data.columns.iter().position(|x| x == &computed_col.name) {
                    cols_to_remove.push(pos);
                }
            }

            // Remove in reverse order to maintain indices
            cols_to_remove.sort_by(|a, b| b.cmp(a));
            for pos in cols_to_remove {
                data.columns.remove(pos);
                for row in &mut data.rows {
                    if pos < row.len() {
                        row.remove(pos);
                    }
                }
            }

            // Re-apply all computed columns
            for computed_col in &self.computed_columns {
                data.columns.push(computed_col.name.clone());

                match &computed_col.column_type {
                    ComputedColumnType::Aggregate(func) => {
                        let value =
                            Self::compute_aggregate_static(data, func, &computed_col.expression)?;
                        for row in &mut data.rows {
                            row.push(value.clone());
                        }
                    }
                    ComputedColumnType::RowOperation(columns_used) => {
                        let expression = computed_col.expression.clone();
                        let cols = columns_used.clone();
                        let mut computed_values = Vec::new();

                        for row in &data.rows {
                            let value =
                                Self::compute_row_operation_static(data, row, &expression, &cols)?;
                            computed_values.push(value);
                        }

                        for (row, value) in data.rows.iter_mut().zip(computed_values) {
                            row.push(value);
                        }
                    }
                    ComputedColumnType::MixedOperation(columns_used, aggregate_expressions) => {
                        let expression = computed_col.expression.clone();
                        let cols = columns_used.clone();
                        let aggs = aggregate_expressions.clone();
                        let mut computed_values = Vec::new();

                        for row in &data.rows {
                            let value = Self::compute_mixed_operation_static(
                                data,
                                row,
                                &expression,
                                &cols,
                                &aggs,
                            )?;
                            computed_values.push(value);
                        }

                        for (row, value) in data.rows.iter_mut().zip(computed_values) {
                            row.push(value);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

pub fn render_ui(frame: &mut Frame, app: &AppState, theme: &Theme) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Min(0),    // Body
            Constraint::Length(3), // Footer
        ])
        .split(frame.area());

    // Header
    let header = Paragraph::new(format!(
        "SQLite Browser - {}",
        std::path::Path::new(&app.db_path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("Unknown")
    ))
    .style(
        Style::default()
            .fg(theme.header)
            .add_modifier(Modifier::BOLD),
    )
    .alignment(Alignment::Center)
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.header)),
    );
    frame.render_widget(header, chunks[0]);

    // Body
    let body_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(25), // Sidebar
            Constraint::Min(0),     // Main area
        ])
        .split(chunks[1]);

    // Render sidebar (tables list)
    render_sidebar(frame, app, body_chunks[0], theme);

    // Render main area
    render_main_area(frame, app, body_chunks[1], theme);

    // Query input overlay
    if app.navigation_mode == NavigationMode::Query {
        render_query_input(frame, app, theme);
    }

    // Edit input overlay
    if app.navigation_mode == NavigationMode::Edit {
        render_edit_input(frame, app, theme);
    }

    // Computed column input overlay
    if app.navigation_mode == NavigationMode::ComputedColumn {
        render_computed_column_input(frame, app, theme);
    }

    // Help overlay
    if app.show_help {
        render_help(frame, theme);
    }

    // Detailed view overlay
    if app.navigation_mode == NavigationMode::DetailedView {
        render_detailed_view(frame, app, theme);
    }

    // Error display overlay
    if app.navigation_mode == NavigationMode::ErrorDisplay {
        render_error_display(frame, app, theme);
    }

    // Footer
    render_footer(frame, app, chunks[2], theme);
}

fn render_sidebar(frame: &mut Frame, app: &AppState, area: Rect, theme: &Theme) {
    let border_style = if app.navigation_mode == NavigationMode::Table {
        Style::default().fg(theme.selected_border)
    } else {
        Style::default().fg(theme.border)
    };

    let title_style = if app.navigation_mode == NavigationMode::Table {
        Style::default()
            .fg(theme.selected_border)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
            .fg(theme.border)
            .add_modifier(Modifier::BOLD)
    };

    let sidebar_title = if app.db_path.ends_with(".xlsx") || app.db_path.ends_with(".xls") {
        "Sheets"
    } else if app.db_path.ends_with(".csv") {
        "Data"
    } else if app.db_path.ends_with(".parquet") {
        "Data"
    } else {
        "Tables"
    };

    let items: Vec<Line> = app
        .tables
        .iter()
        .enumerate()
        .map(|(i, table)| {
            if i == app.selected_table_idx {
                if app.navigation_mode == NavigationMode::Table {
                    Line::from(Span::styled(
                        format!("▶ {}", table),
                        Style::default()
                            .fg(theme.selected_border)
                            .add_modifier(Modifier::BOLD),
                    ))
                } else {
                    Line::from(Span::styled(
                        format!("▶ {}", table),
                        Style::default().fg(Color::DarkGray),
                    ))
                }
            } else {
                Line::from(Span::styled(
                    format!("  {}", table),
                    Style::default().fg(theme.text),
                ))
            }
        })
        .collect();

    let list = Paragraph::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .title(Span::styled(sidebar_title, title_style)),
    );

    frame.render_widget(list, area);
}

fn render_main_area(frame: &mut Frame, app: &AppState, area: Rect, theme: &Theme) {
    if app.tables.is_empty() || app.selected_table_idx >= app.tables.len() {
        let placeholder = Paragraph::new("Select a table to view its contents")
            .style(Style::default().fg(Color::DarkGray))
            .alignment(Alignment::Center)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Table Contents")
                    .border_style(Style::default().fg(theme.border)),
            );
        frame.render_widget(placeholder, area);
        return;
    }

    let border_style = match app.navigation_mode {
        NavigationMode::Data => Style::default().fg(theme.selected_border),
        NavigationMode::Edit => Style::default().fg(theme.edit_border),
        _ => Style::default().fg(theme.border),
    };

    let title_style = match app.navigation_mode {
        NavigationMode::Data => Style::default()
            .fg(theme.selected_border)
            .add_modifier(Modifier::BOLD),
        NavigationMode::Edit => Style::default()
            .fg(theme.edit_border)
            .add_modifier(Modifier::BOLD),
        _ => Style::default()
            .fg(theme.border)
            .add_modifier(Modifier::BOLD),
    };

    if let Some(data) = &app.current_data {
        let table_name = &app.tables[app.selected_table_idx];

        // Calculate pagination info
        let current_page = (app.data_offset / app.page_size) + 1;
        let total_pages = (data.total_rows + app.page_size - 1) / app.page_size.max(1);
        let start_row = app.data_offset + 1;
        let end_row = (app.data_offset + data.rows.len()).min(data.total_rows);

        let mut title = format!(
            "Table: {} | Total: {} rows | Columns: {}",
            table_name,
            data.total_rows,
            data.columns.len()
        );

        if total_pages > 1 {
            title.push_str(&format!(
                " | Page {}/{} | Rows {}-{}",
                current_page, total_pages, start_row, end_row
            ));
        }

        if app.current_query.is_some() {
            title.push_str(" | Custom Query");
        }

        if app.data_modified {
            title.push_str(" | *MODIFIED*");
        }

        // Create table rows (skip rowid column for display)
        let col_offset = if !data.columns.is_empty() && data.columns[0] == "rowid" {
            1
        } else {
            0
        };
        let rows: Vec<Row> = data
            .rows
            .iter()
            .enumerate()
            .map(|(i, row_data)| {
                let display_row = if col_offset > 0 && row_data.len() > col_offset {
                    &row_data[col_offset..]
                } else {
                    row_data
                };

                let cells: Vec<Cell> = display_row
                    .iter()
                    .enumerate()
                    .map(|(j, cell)| {
                        let actual_col_idx = j + col_offset;
                        let content = if cell.len() > 40 {
                            format!("{}...", &cell[..37])
                        } else {
                            cell.clone()
                        };

                        // Highlight selected cell in Edit mode or Data mode
                        if (app.navigation_mode == NavigationMode::Edit
                            || app.navigation_mode == NavigationMode::Data)
                            && i == app.selected_row_idx
                            && actual_col_idx == app.selected_col_idx
                        {
                            if app.navigation_mode == NavigationMode::Edit {
                                Cell::from(content).style(
                                    Style::default()
                                        .fg(theme.edit_text)
                                        .bg(theme.edit_bg)
                                        .add_modifier(Modifier::BOLD),
                                )
                            } else {
                                Cell::from(content).style(
                                    Style::default()
                                        .fg(theme.selected_text)
                                        .bg(theme.selected_bg)
                                        .add_modifier(Modifier::BOLD),
                                )
                            }
                        } else {
                            Cell::from(content).style(Style::default().fg(theme.text))
                        }
                    })
                    .collect();

                Row::new(cells)
            })
            .collect();

        // Create column widths (for display columns only)
        let display_col_count = if !data.columns.is_empty() && data.columns[0] == "rowid" {
            data.columns.len() - 1
        } else {
            data.columns.len()
        };
        let widths: Vec<Constraint> = (0..display_col_count)
            .map(|_| Constraint::Percentage(100 / display_col_count.max(1) as u16))
            .collect();

        // Skip rowid column for display
        let display_columns = if !data.columns.is_empty() && data.columns[0] == "rowid" {
            &data.columns[1..]
        } else {
            &data.columns[..]
        };

        let col_offset = if !data.columns.is_empty() && data.columns[0] == "rowid" {
            1
        } else {
            0
        };

        let table = Table::new(rows, widths)
            .header(Row::new(
                display_columns
                    .iter()
                    .map(|h| {
                        // Check if this is a computed column
                        let is_computed = app.computed_columns.iter().any(|col| &col.name == h);
                        if is_computed {
                            let header_text = format!("*{}", h);
                            Cell::from(header_text).style(
                                Style::default()
                                    .fg(theme.number)
                                    .add_modifier(Modifier::BOLD),
                            )
                        } else {
                            Cell::from(h.as_str()).style(
                                Style::default()
                                    .fg(theme.column_header)
                                    .add_modifier(Modifier::BOLD),
                            )
                        }
                    })
                    .collect::<Vec<_>>(),
            ))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(Span::styled(title, title_style))
                    .border_style(border_style),
            )
            .style(Style::default().fg(theme.text));

        frame.render_widget(table, area);
    } else {
        let placeholder = Paragraph::new("Loading...")
            .style(Style::default().fg(Color::DarkGray))
            .alignment(Alignment::Center)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Table Contents")
                    .border_style(border_style),
            );
        frame.render_widget(placeholder, area);
    }
}

fn render_query_input(frame: &mut Frame, app: &AppState, theme: &Theme) {
    let area = frame.area();
    let popup_area = Rect {
        x: area.width / 6,
        y: area.height / 2 - 2,
        width: area.width * 2 / 3,
        height: 5,
    };

    // Clear the background area first
    frame.render_widget(Clear, popup_area);

    let query_input = Paragraph::new(format!("{}_", app.query_input))
        .style(Style::default().fg(theme.query_text).bg(theme.query_bg))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Enter SQL Query (ESC to cancel)")
                .border_style(Style::default().fg(theme.query_border))
                .style(Style::default().bg(theme.query_bg)),
        );

    frame.render_widget(query_input, popup_area);
}

fn render_edit_input(frame: &mut Frame, app: &AppState, theme: &Theme) {
    let area = frame.area();
    let popup_area = Rect {
        x: area.width / 6,
        y: area.height.saturating_sub(7),
        width: area.width * 2 / 3,
        height: 3,
    };

    // Clear the background area first
    frame.render_widget(Clear, popup_area);

    let edit_input = Paragraph::new(format!("{}_", app.edit_input))
        .style(Style::default().fg(theme.edit_text).bg(theme.edit_area_bg))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(theme.edit_border))
                .style(Style::default().bg(theme.edit_area_bg)),
        );

    frame.render_widget(edit_input, popup_area);
}

fn render_computed_column_input(frame: &mut Frame, app: &AppState, theme: &Theme) {
    let area = frame.area();
    let popup_area = Rect {
        x: area.width / 6,
        y: area.height / 2 - 2,
        width: area.width * 2 / 3,
        height: 5,
    };

    // Clear the background area first
    frame.render_widget(Clear, popup_area);

    let computed_col_input = Paragraph::new(format!("{}_", app.computed_column_input))
        .style(Style::default().fg(theme.query_text).bg(theme.query_bg))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Computed Column (e.g., sum(Age), column1=Age*2)")
                .border_style(Style::default().fg(theme.query_border))
                .style(Style::default().bg(theme.query_bg)),
        );

    frame.render_widget(computed_col_input, popup_area);
}

fn render_detailed_view(frame: &mut Frame, app: &AppState, theme: &Theme) {
    let area = frame.area();
    let popup_area = Rect {
        x: area.width / 8,
        y: area.height / 8,
        width: area.width * 3 / 4,
        height: area.height * 3 / 4,
    };

    // Clear the background area first
    frame.render_widget(Clear, popup_area);

    if let Some(data) = &app.current_data {
        if let Some(row_idx) = app.detailed_view_row {
            if row_idx < data.rows.len() {
                let row_data = &data.rows[row_idx];
                let table_name = &app.tables[app.selected_table_idx];

                // Calculate row number for display (1-based)
                let display_row_num = app.data_offset + row_idx + 1;

                let mut lines = vec![
                    Line::from(Span::styled(
                        format!("Row {} Details - {}", display_row_num, table_name),
                        Style::default()
                            .fg(theme.detailed_view_title)
                            .add_modifier(Modifier::BOLD),
                    )),
                    Line::from(""),
                ];

                // Add each field with its value
                for (i, (column, value)) in data.columns.iter().zip(row_data.iter()).enumerate() {
                    let is_selected = i == app.detailed_view_selected_field;

                    let field_style = if is_selected {
                        Style::default()
                            .fg(theme.selected_text)
                            .bg(theme.selected_bg)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default()
                            .fg(theme.detailed_view_field)
                            .add_modifier(Modifier::BOLD)
                    };

                    let value_style = if is_selected {
                        Style::default()
                            .fg(theme.selected_text)
                            .bg(theme.selected_bg)
                    } else {
                        Style::default().fg(theme.detailed_view_value)
                    };

                    lines.push(Line::from(vec![
                        Span::styled(format!("{}: ", column), field_style),
                        Span::styled(value, value_style),
                    ]));

                    if i < data.columns.len() - 1 {
                        lines.push(Line::from(""));
                    }
                }

                lines.push(Line::from(""));
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    "↑↓ Navigate fields | c Copy value | ESC Close",
                    Style::default().fg(Color::DarkGray),
                )));

                let detailed_view = Paragraph::new(lines)
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .title("Detailed View")
                            .border_style(Style::default().fg(theme.detailed_view_border))
                            .style(Style::default().bg(theme.detailed_view_bg)),
                    )
                    .style(
                        Style::default()
                            .fg(theme.detailed_view_value)
                            .bg(theme.detailed_view_bg),
                    )
                    .wrap(ratatui::widgets::Wrap { trim: false });

                frame.render_widget(detailed_view, popup_area);
            }
        }
    }
}

fn render_error_display(frame: &mut Frame, app: &AppState, theme: &Theme) {
    let area = frame.area();
    let popup_area = Rect {
        x: area.width / 6,
        y: area.height / 3,
        width: area.width * 2 / 3,
        height: area.height / 3,
    };

    // Clear the background area first
    frame.render_widget(Clear, popup_area);

    if let Some(error_msg) = &app.error_message {
        let lines = vec![
            Line::from(Span::styled(
                "Error",
                Style::default()
                    .fg(theme.error)
                    .add_modifier(Modifier::BOLD),
            )),
            Line::from(""),
            Line::from(Span::styled(error_msg, Style::default().fg(theme.text))),
            Line::from(""),
            Line::from(Span::styled(
                "Press ESC to close",
                Style::default().fg(Color::DarkGray),
            )),
        ];

        let error_display = Paragraph::new(lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Error")
                    .border_style(Style::default().fg(theme.error))
                    .style(Style::default().bg(Color::Black)),
            )
            .style(Style::default().fg(theme.text).bg(Color::Black))
            .alignment(Alignment::Center)
            .wrap(ratatui::widgets::Wrap { trim: false });

        frame.render_widget(error_display, popup_area);
    }
}

fn render_help(frame: &mut Frame, theme: &Theme) {
    let area = frame.area();
    let popup_area = Rect {
        x: area.width / 8,
        y: area.height / 8,
        width: area.width * 3 / 4,
        height: area.height * 3 / 4,
    };

    let help_line = |key: &str, desc: &str, theme: &Theme| -> Line {
        Line::from(vec![
            Span::styled(
                key.to_string(),
                Style::default()
                    .fg(theme.help_key)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("       ".to_string(), Style::default()), // spacing
            Span::styled(
                desc.to_string(),
                Style::default().fg(theme.help_description),
            ),
        ])
    };

    let help_text = vec![
        Line::from(Span::styled(
            "SQLite Browser - Help",
            Style::default()
                .fg(theme.help_title)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(Span::styled(
            "Table Navigation Mode:",
            Style::default()
                .fg(theme.help_section_header)
                .add_modifier(Modifier::BOLD),
        )),
        help_line("  ↑↓", "Navigate tables", theme),
        help_line("  →/Enter", "Enter table data view", theme),
        help_line("  h", "Toggle this help", theme),
        help_line("  Ctrl+C", "Exit application", theme),
        Line::from(""),
        Line::from(Span::styled(
            "Data Navigation Mode:",
            Style::default()
                .fg(theme.help_section_header)
                .add_modifier(Modifier::BOLD),
        )),
        help_line("  ↑↓←→", "Navigate rows and columns", theme),
        help_line("  ←", "Back to table list (when at first column)", theme),
        help_line("  Space", "Enter edit mode for selected cell", theme),
        help_line("  Enter", "Show detailed view for selected row", theme),
        help_line("  n", "Add new row", theme),
        help_line("  PgUp/Dn", "Page navigation", theme),
        help_line("  Home", "Go to first page", theme),
        help_line("  End", "Go to last page", theme),
        help_line("  i", "Enter query mode (SQLite only)", theme),
        help_line("  =", "Add computed column (name=expression)", theme),
        help_line("  e", "Export to CSV", theme),
        help_line("  s", "Save changes", theme),
        help_line("  r", "Refresh data", theme),
        help_line("  h", "Toggle this help", theme),
        help_line("  Ctrl+C", "Exit application", theme),
        Line::from(""),
        Line::from(Span::styled(
            "Edit Mode:",
            Style::default()
                .fg(theme.help_section_header)
                .add_modifier(Modifier::BOLD),
        )),
        help_line("  Type", "Edit cell content", theme),
        help_line("  ↑↓←→", "Navigate between cells while editing", theme),
        help_line("  Enter", "Save changes and exit edit mode", theme),
        help_line("  Tab", "Save and move to next cell", theme),
        help_line("  Ctrl+N", "Add new row", theme),
        help_line("  ESC", "Cancel edit", theme),
        Line::from(""),
        Line::from(Span::styled(
            "Query Mode:",
            Style::default()
                .fg(theme.help_section_header)
                .add_modifier(Modifier::BOLD),
        )),
        help_line("  Type", "Type your SQL query", theme),
        help_line("  Enter", "Execute query", theme),
        help_line("  ESC", "Cancel query", theme),
        Line::from(""),
        Line::from(Span::styled(
            "Detailed View Mode:",
            Style::default()
                .fg(theme.help_section_header)
                .add_modifier(Modifier::BOLD),
        )),
        help_line("  ↑↓", "Navigate between fields", theme),
        help_line("  c", "Copy selected field value to clipboard", theme),
        help_line("  ESC", "Close detailed view", theme),
        Line::from(""),
        Line::from(Span::styled(
            "Computed Column Mode:",
            Style::default()
                .fg(theme.help_section_header)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            "  Type expression like sum(Age), Age + Height, column1=25*2",
            Style::default().fg(theme.help_description),
        )),
        Line::from(Span::styled(
            "  Use name=expression to create named columns",
            Style::default().fg(theme.help_description),
        )),
        Line::from(Span::styled(
            "  Supported: sum, mean, count, min, max, +, -, *, /, constants",
            Style::default().fg(theme.help_description),
        )),
        help_line("  Enter", "Add computed column", theme),
        help_line("  ESC", "Cancel", theme),
        Line::from(""),
        Line::from(Span::styled(
            "Press 'h' to close this help",
            Style::default().fg(theme.help_description),
        )),
    ];

    // Clear the background area first
    frame.render_widget(Clear, popup_area);

    let help = Paragraph::new(help_text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Help")
                .border_style(Style::default().fg(theme.help))
                .style(Style::default().bg(theme.help_bg)),
        )
        .style(
            Style::default()
                .fg(theme.help_description)
                .bg(theme.help_bg),
        );

    frame.render_widget(help, popup_area);
}

fn render_footer(frame: &mut Frame, app: &AppState, area: Rect, theme: &Theme) {
    let footer_text = match app.navigation_mode {
        NavigationMode::Table => "↑↓ Navigate | → Enter | h Help | Ctrl+C Exit",
        NavigationMode::Data => "↑↓←→ Navigate | ← Back | Space Edit | Enter Details | n New Row | PgUp/Dn Page | i Query | = Computed | e Export | s Save | h Help | Ctrl+C Exit",
        NavigationMode::Query => "Type query | Enter Execute | ESC Cancel",
        NavigationMode::Edit => "Type to edit | ↑↓←→ Navigate | Enter Save | Tab Next | Ctrl+N New Row | ESC Cancel",
        NavigationMode::DetailedView => "↑↓ Navigate fields | c Copy value | ESC Close",
        NavigationMode::ErrorDisplay => "ESC Close error",
        NavigationMode::ComputedColumn => "Type expression | Enter Add | ESC Cancel",
    };

    let mut footer_content = vec![Line::from(Span::styled(
        footer_text,
        Style::default().fg(Color::DarkGray),
    ))];

    if let Some(status) = &app.status_message {
        footer_content.insert(
            0,
            Line::from(Span::styled(status, Style::default().fg(theme.status))),
        );
    }

    let footer = Paragraph::new(footer_content)
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(theme.border)),
        );

    frame.render_widget(footer, area);
}
