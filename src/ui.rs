use anyhow::Result;
use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, TableState},
    Frame,
};
use arboard::Clipboard;

use crate::data_source::DataSource;
use crate::database::QueryResult;

#[derive(Debug, Clone, PartialEq)]
pub enum NavigationMode {
    Table,
    Data,
    Query,
    Edit,
    DetailedView,
    ErrorDisplay,
}

#[derive(Debug, Clone, PartialEq)]
enum MoveTo {
    Up,
    Down,
    Left,
    Right,
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
    pub clipboard: Option<Clipboard>, // Persistent clipboard state
    pub error_message: Option<String>, // Error message to display
    pub previous_navigation_mode: NavigationMode, // Previous mode before error display
}

impl AppState {
    pub fn new(db_path: String, tables: Vec<String>) -> Self {
        Self {
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
        }
    }

    pub fn current_table(&self) -> Option<&str> {
        self.tables.get(self.selected_table_idx).map(|s| s.as_str())
    }

    pub fn handle_key_event(&mut self, key_event: KeyEvent, data_source: &DataSource) -> Result<bool> {
        match self.navigation_mode {
            NavigationMode::Query => self.handle_query_input(key_event, data_source),
            NavigationMode::Table => self.handle_table_navigation(key_event, data_source),
            NavigationMode::Data => self.handle_data_navigation(key_event, data_source),
            NavigationMode::Edit => self.handle_edit_mode(key_event, data_source),
            NavigationMode::DetailedView => self.handle_detailed_view(key_event, data_source),
            NavigationMode::ErrorDisplay => self.handle_error_display(key_event, data_source),
        }
    }

    fn handle_query_input(&mut self, key_event: KeyEvent, data_source: &DataSource) -> Result<bool> {
        match key_event.code {
            KeyCode::Esc => {
                self.navigation_mode = NavigationMode::Data;
                self.query_input.clear();
            }
            KeyCode::Enter => {
                if !self.query_input.trim().is_empty() {
                    if let Some(table_name) = self.current_table() {
                        if data_source.supports_custom_queries() {
                            match data_source.execute_custom_query(&self.query_input, table_name, 0, self.page_size) {
                                Ok(result) => {
                                    self.current_query = Some(self.query_input.clone());
                                    self.current_data = Some(result);
                                    self.selected_row_idx = 0;
                                    self.data_offset = 0;
                                    self.status_message = Some("Query executed successfully".to_string());
                                }
                                Err(e) => {
                                    self.show_error(format!("Query error: {}", e));
                                }
                            }
                        } else {
                            self.status_message = Some("Custom queries not supported for this file type".to_string());
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

    fn handle_table_navigation(&mut self, key_event: KeyEvent, data_source: &DataSource) -> Result<bool> {
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
            KeyCode::Char('q') | KeyCode::Char('c') if key_event.modifiers.contains(KeyModifiers::CONTROL) => {
                return Ok(false);
            }
            KeyCode::Char('h') => {
                self.show_help = !self.show_help;
            }
            _ => {}
        }
        Ok(true)
    }

    fn handle_data_navigation(&mut self, key_event: KeyEvent, data_source: &DataSource) -> Result<bool> {
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
                    let min_col = if !data.columns.is_empty() && data.columns[0] == "rowid" { 1 } else { 0 };
                    if self.selected_col_idx > min_col {
                        self.selected_col_idx -= 1;
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
                    if self.selected_row_idx < data.rows.len() && self.selected_col_idx < data.columns.len() {
                        // Prevent editing rowid column (column 0)
                        if !data.columns.is_empty() && data.columns[0] == "rowid" && self.selected_col_idx == 0 {
                            self.show_error("Cannot edit rowid column".to_string());
                            return Ok(true);
                        }
                        
                        self.navigation_mode = NavigationMode::Edit;
                        self.editing_cell = Some((self.selected_row_idx, self.selected_col_idx));
                        self.edit_input = data.rows[self.selected_row_idx][self.selected_col_idx].clone();
                    }
                }
            }
            KeyCode::Tab => {
                self.navigation_mode = NavigationMode::Table;
                self.reset_data_view();
                self.load_current_data(data_source)?;
            }
            KeyCode::Char('i') => {
                self.navigation_mode = NavigationMode::Query;
                self.query_input.clear();
            }
            KeyCode::Char('e') => {
                self.export_to_csv(data_source)?;
            }
            KeyCode::Char('s') => {
                // If we're in a custom query, warn user to go back to table view
                if self.current_query.is_some() {
                    self.show_error("Cannot save custom query results. Press 'r' to reload table data first.".to_string());
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
            KeyCode::Char('q') | KeyCode::Char('c') if key_event.modifiers.contains(KeyModifiers::CONTROL) => {
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
                            if !data.columns.is_empty() && data.columns[0] == "rowid" && col_idx == 0 {
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
                    let mut new_row: Vec<String> = data.columns.iter().map(|_| String::new()).collect();
                    // Set rowid to empty for new rows (will be handled by INSERT)
                    if !data.columns.is_empty() && data.columns[0] == "rowid" {
                        new_row[0] = String::new();
                    }
                    
                    data.rows.push(new_row);
                    data.total_rows += 1;
                    self.data_modified = true;
                    self.selected_row_idx = data.rows.len() - 1;
                    self.selected_col_idx = if data.columns.is_empty() || data.columns[0] != "rowid" { 0 } else { 1 };
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
                            if !data.columns.is_empty() && data.columns[0] == "rowid" && col_idx == 0 {
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
                                let min_col = if !data.columns.is_empty() && data.columns[0] == "rowid" { 1 } else { 0 };
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

    fn save_current_edit_and_move_to(&mut self, direction: MoveTo, data_source: &DataSource) -> Result<()> {
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
                    let min_col = if !data.columns.is_empty() && data.columns[0] == "rowid" { 1 } else { 0 };
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
            let min_col = if !data.columns.is_empty() && data.columns[0] == "rowid" { 1 } else { 0 };
            if self.selected_col_idx < min_col {
                self.selected_col_idx = min_col;
            }
        }
    }

    pub fn load_current_data(&mut self, data_source: &DataSource) -> Result<()> {
        if let Some(table_name) = self.current_table() {
            let result = if let Some(query) = &self.current_query {
                data_source.execute_custom_query(query, table_name, self.data_offset, self.page_size)?
            } else {
                data_source.get_table_data(table_name, self.data_offset, self.page_size)?
            };
            
            // Store original data for comparison when saving
            self.original_data = Some(result.clone());
            self.current_data = Some(result);
            
            // Ensure column selection is valid (skip rowid)
            self.ensure_valid_col_selection();
        }
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
                        self.status_message = Some(format!("Changes saved to {} (converted from Excel)", filename));
                    }
                    crate::data_source::DataSource::Sqlite(_) => {
                        let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
                        let filename = format!("{}_edited_{}.csv", table_name, timestamp);
                        self.write_csv_data(&data, &filename)?;
                        self.data_modified = false;
                        self.status_message = Some(format!("Changes exported to {} (SQLite direct save not supported)", filename));
                    }
                    crate::data_source::DataSource::Parquet(_, _) => {
                        let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
                        let filename = format!("{}_edited_{}.csv", table_name, timestamp);
                        self.write_csv_data(&data, &filename)?;
                        self.data_modified = false;
                        self.status_message = Some(format!("Changes saved to {} (converted from Parquet)", filename));
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

    fn handle_detailed_view(&mut self, key_event: KeyEvent, _data_source: &DataSource) -> Result<bool> {
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
                        if row_idx < data.rows.len() && self.detailed_view_selected_field < data.columns.len() {
                            let value = data.rows[row_idx][self.detailed_view_selected_field].clone();
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
            KeyCode::Char('q') | KeyCode::Char('c') if key_event.modifiers.contains(KeyModifiers::CONTROL) => {
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

    fn handle_error_display(&mut self, key_event: KeyEvent, _data_source: &DataSource) -> Result<bool> {
        match key_event.code {
            KeyCode::Esc => {
                self.navigation_mode = self.previous_navigation_mode.clone();
                self.error_message = None;
            }
            KeyCode::Char('q') | KeyCode::Char('c') if key_event.modifiers.contains(KeyModifiers::CONTROL) => {
                return Ok(false);
            }
            _ => {}
        }
        Ok(true)
    }

}

pub fn render_ui(frame: &mut Frame, app: &AppState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Min(0),    // Body
            Constraint::Length(3), // Footer
        ])
        .split(frame.area());

    // Header
    let header = Paragraph::new(format!("SQLite Browser - {}", 
        std::path::Path::new(&app.db_path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("Unknown")))
        .style(Style::default().fg(Color::Green).add_modifier(Modifier::BOLD))
        .alignment(Alignment::Center)
        .block(Block::default().borders(Borders::ALL).border_style(Style::default().fg(Color::Green)));
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
    render_sidebar(frame, app, body_chunks[0]);
    
    // Render main area
    render_main_area(frame, app, body_chunks[1]);

    // Query input overlay
    if app.navigation_mode == NavigationMode::Query {
        render_query_input(frame, app);
    }

    // Edit input overlay
    if app.navigation_mode == NavigationMode::Edit {
        render_edit_input(frame, app);
    }

    // Help overlay
    if app.show_help {
        render_help(frame);
    }

    // Detailed view overlay
    if app.navigation_mode == NavigationMode::DetailedView {
        render_detailed_view(frame, app);
    }

    // Error display overlay
    if app.navigation_mode == NavigationMode::ErrorDisplay {
        render_error_display(frame, app);
    }

    // Footer
    render_footer(frame, app, chunks[2]);
}

fn render_sidebar(frame: &mut Frame, app: &AppState, area: Rect) {
    let border_style = if app.navigation_mode == NavigationMode::Table {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::Cyan)
    };

    let title_style = if app.navigation_mode == NavigationMode::Table {
        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)
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

    let items: Vec<Line> = app.tables.iter().enumerate().map(|(i, table)| {
        if i == app.selected_table_idx {
            if app.navigation_mode == NavigationMode::Table {
                Line::from(Span::styled(format!("▶ {}", table), Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)))
            } else {
                Line::from(Span::styled(format!("▶ {}", table), Style::default().fg(Color::DarkGray)))
            }
        } else {
            Line::from(Span::raw(format!("  {}", table)))
        }
    }).collect();

    let list = Paragraph::new(items)
        .block(Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .title(Span::styled(sidebar_title, title_style)));
    
    frame.render_widget(list, area);
}

fn render_main_area(frame: &mut Frame, app: &AppState, area: Rect) {
    if app.tables.is_empty() || app.selected_table_idx >= app.tables.len() {
        let placeholder = Paragraph::new("Select a table to view its contents")
            .style(Style::default().fg(Color::DarkGray))
            .alignment(Alignment::Center)
            .block(Block::default()
                .borders(Borders::ALL)
                .title("Table Contents")
                .border_style(Style::default().fg(Color::Green)));
        frame.render_widget(placeholder, area);
        return;
    }

    let border_style = match app.navigation_mode {
        NavigationMode::Data => Style::default().fg(Color::Yellow),
        NavigationMode::Edit => Style::default().fg(Color::Red),
        _ => Style::default().fg(Color::Green),
    };

    let title_style = match app.navigation_mode {
        NavigationMode::Data => Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        NavigationMode::Edit => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        _ => Style::default().fg(Color::Green).add_modifier(Modifier::BOLD),
    };

    if let Some(data) = &app.current_data {
        let table_name = &app.tables[app.selected_table_idx];
        
        // Calculate pagination info
        let current_page = (app.data_offset / app.page_size) + 1;
        let total_pages = (data.total_rows + app.page_size - 1) / app.page_size.max(1);
        let start_row = app.data_offset + 1;
        let end_row = (app.data_offset + data.rows.len()).min(data.total_rows);

        let mut title = format!("Table: {} | Total: {} rows | Columns: {}", 
            table_name, data.total_rows, data.columns.len());
        
        if total_pages > 1 {
            title.push_str(&format!(" | Page {}/{} | Rows {}-{}", 
                current_page, total_pages, start_row, end_row));
        }

        if app.current_query.is_some() {
            title.push_str(" | Custom Query");
        }
        
        if app.data_modified {
            title.push_str(" | *MODIFIED*");
        }

        // Create table rows (skip rowid column for display)
        let col_offset = if !data.columns.is_empty() && data.columns[0] == "rowid" { 1 } else { 0 };
        let rows: Vec<Row> = data.rows.iter().enumerate().map(|(i, row_data)| {
            let display_row = if col_offset > 0 && row_data.len() > col_offset {
                &row_data[col_offset..]
            } else {
                row_data
            };
            
            let cells: Vec<Cell> = display_row.iter().enumerate().map(|(j, cell)| {
                let actual_col_idx = j + col_offset;
                let content = if cell.len() > 40 {
                    format!("{}...", &cell[..37])
                } else {
                    cell.clone()
                };
                
                // Highlight selected cell in Edit mode or Data mode
                if (app.navigation_mode == NavigationMode::Edit || app.navigation_mode == NavigationMode::Data) 
                    && i == app.selected_row_idx && actual_col_idx == app.selected_col_idx {
                    if app.navigation_mode == NavigationMode::Edit {
                        Cell::from(content).style(Style::default().fg(Color::Black).bg(Color::Yellow).add_modifier(Modifier::BOLD))
                    } else {
                        Cell::from(content).style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))
                    }
                } else {
                    Cell::from(content)
                }
            }).collect();

            if app.navigation_mode == NavigationMode::Data && i == app.selected_row_idx {
                Row::new(cells).style(Style::default().fg(Color::Yellow))
            } else {
                Row::new(cells)
            }
        }).collect();

        // Create column widths (for display columns only)
        let display_col_count = if !data.columns.is_empty() && data.columns[0] == "rowid" {
            data.columns.len() - 1
        } else {
            data.columns.len()
        };
        let widths: Vec<Constraint> = (0..display_col_count).map(|_| Constraint::Percentage(100 / display_col_count.max(1) as u16)).collect();

        // Skip rowid column for display
        let display_columns = if !data.columns.is_empty() && data.columns[0] == "rowid" {
            &data.columns[1..]
        } else {
            &data.columns[..]
        };
        
        let col_offset = if !data.columns.is_empty() && data.columns[0] == "rowid" { 1 } else { 0 };
        
        let table = Table::new(rows, widths)
            .header(Row::new(display_columns.iter().map(|h| {
                Cell::from(h.as_str()).style(Style::default().fg(Color::Magenta).add_modifier(Modifier::BOLD))
            }).collect::<Vec<_>>()))
            .block(Block::default()
                .borders(Borders::ALL)
                .title(Span::styled(title, title_style))
                .border_style(border_style))
            .style(Style::default().fg(Color::Cyan));

        frame.render_widget(table, area);
    } else {
        let placeholder = Paragraph::new("Loading...")
            .style(Style::default().fg(Color::DarkGray))
            .alignment(Alignment::Center)
            .block(Block::default()
                .borders(Borders::ALL)
                .title("Table Contents")
                .border_style(border_style));
        frame.render_widget(placeholder, area);
    }
}

fn render_query_input(frame: &mut Frame, app: &AppState) {
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
        .style(Style::default().fg(Color::White).bg(Color::Black))
        .block(Block::default()
            .borders(Borders::ALL)
            .title("Enter SQL Query (ESC to cancel)")
            .border_style(Style::default().fg(Color::Cyan))
            .style(Style::default().bg(Color::Black)));

    frame.render_widget(query_input, popup_area);
}

fn render_edit_input(frame: &mut Frame, app: &AppState) {
    let area = frame.area();
    let popup_area = Rect {
        x: area.width / 6,
        y: area.height.saturating_sub(5),
        width: area.width * 2 / 3,
        height: 3,
    };

    // Clear the background area first
    frame.render_widget(Clear, popup_area);

    let edit_input = Paragraph::new(format!("{}_", app.edit_input))
        .style(Style::default().fg(Color::Black).bg(Color::White))
        .block(Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Yellow))
            .style(Style::default().bg(Color::White)));

    frame.render_widget(edit_input, popup_area);
}

fn render_detailed_view(frame: &mut Frame, app: &AppState) {
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
                        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
                    )),
                    Line::from(""),
                ];

                // Add each field with its value
                for (i, (column, value)) in data.columns.iter().zip(row_data.iter()).enumerate() {
                    let is_selected = i == app.detailed_view_selected_field;
                    
                    let field_style = if is_selected {
                        Style::default().fg(Color::Black).bg(Color::Yellow).add_modifier(Modifier::BOLD)
                    } else {
                        Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)
                    };
                    
                    let value_style = if is_selected {
                        Style::default().fg(Color::Black).bg(Color::Yellow)
                    } else {
                        Style::default().fg(Color::White)
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
                lines.push(Line::from(Span::styled("↑↓ Navigate fields | c Copy value | ESC Close", Style::default().fg(Color::DarkGray))));

                let detailed_view = Paragraph::new(lines)
                    .block(Block::default()
                        .borders(Borders::ALL)
                        .title("Detailed View")
                        .border_style(Style::default().fg(Color::Yellow))
                        .style(Style::default().bg(Color::Black)))
                    .style(Style::default().fg(Color::White).bg(Color::Black))
                    .wrap(ratatui::widgets::Wrap { trim: false });

                frame.render_widget(detailed_view, popup_area);
            }
        }
    }
}

fn render_error_display(frame: &mut Frame, app: &AppState) {
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
            Line::from(Span::styled("Error", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD))),
            Line::from(""),
            Line::from(Span::styled(error_msg, Style::default().fg(Color::White))),
            Line::from(""),
            Line::from(Span::styled("Press ESC to close", Style::default().fg(Color::DarkGray))),
        ];

        let error_display = Paragraph::new(lines)
            .block(Block::default()
                .borders(Borders::ALL)
                .title("Error")
                .border_style(Style::default().fg(Color::Red))
                .style(Style::default().bg(Color::Black)))
            .style(Style::default().fg(Color::White).bg(Color::Black))
            .alignment(Alignment::Center)
            .wrap(ratatui::widgets::Wrap { trim: false });

        frame.render_widget(error_display, popup_area);
    }
}

fn render_help(frame: &mut Frame) {
    let area = frame.area();
    let popup_area = Rect {
        x: area.width / 8,
        y: area.height / 8,
        width: area.width * 3 / 4,
        height: area.height * 3 / 4,
    };

    let help_text = vec![
        Line::from(Span::styled("SQLite Browser - Help", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))),
        Line::from(""),
        Line::from(Span::styled("Table Navigation Mode:", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD))),
        Line::from("  ↑↓      Navigate tables"),
        Line::from("  →/Enter Enter table data view"),
        Line::from("  h       Toggle this help"),
        Line::from("  Ctrl+C  Exit application"),
        Line::from(""),
        Line::from(Span::styled("Data Navigation Mode:", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD))),
        Line::from("  ↑↓←→    Navigate rows and columns"),
        Line::from("  Space   Enter edit mode for selected cell"),
        Line::from("  Enter   Show detailed view for selected row"),
        Line::from("  PgUp/Dn Page navigation"),
        Line::from("  Home    Go to first page"),
        Line::from("  End     Go to last page"),
        Line::from("  Tab     Back to table/sheet list"),
        Line::from("  i       Enter query mode (SQLite only)"),
        Line::from("  e       Export to CSV"),
        Line::from("  s       Save changes"),
        Line::from("  r       Refresh data"),
        Line::from("  h       Toggle this help"),
        Line::from("  Ctrl+C  Exit application"),
        Line::from(""),
        Line::from(Span::styled("Edit Mode:", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD))),
        Line::from("  Type    Edit cell content"),
        Line::from("  ↑↓←→    Navigate between cells while editing"),
        Line::from("  Enter   Save changes and exit edit mode"),
        Line::from("  Tab     Save and move to next cell"),
        Line::from("  Ctrl+N  Add new row"),
        Line::from("  ESC     Cancel edit"),
        Line::from(""),
        Line::from(Span::styled("Query Mode:", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD))),
        Line::from("  Type your SQL query"),
        Line::from("  Enter   Execute query"),
        Line::from("  ESC     Cancel query"),
        Line::from(""),
        Line::from(Span::styled("Detailed View Mode:", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD))),
        Line::from("  ↑↓      Navigate between fields"),
        Line::from("  c       Copy selected field value to clipboard"),
        Line::from("  ESC     Close detailed view"),
        Line::from(""),
        Line::from("Press 'h' to close this help"),
    ];

    // Clear the background area first
    frame.render_widget(Clear, popup_area);

    let help = Paragraph::new(help_text)
        .block(Block::default()
            .borders(Borders::ALL)
            .title("Help")
            .border_style(Style::default().fg(Color::Yellow))
            .style(Style::default().bg(Color::Black)))
        .style(Style::default().fg(Color::White).bg(Color::Black));

    frame.render_widget(help, popup_area);
}

fn render_footer(frame: &mut Frame, app: &AppState, area: Rect) {
    let footer_text = match app.navigation_mode {
        NavigationMode::Table => "↑↓ Navigate | → Enter | h Help | Ctrl+C Exit",
        NavigationMode::Data => "↑↓←→ Navigate | Space Edit | Enter Details | PgUp/Dn Page | Tab Back | i Query | e Export | s Save | h Help | Ctrl+C Exit",
        NavigationMode::Query => "Type query | Enter Execute | ESC Cancel",
        NavigationMode::Edit => "Type to edit | ↑↓←→ Navigate | Enter Save | Tab Next | Ctrl+N New Row | ESC Cancel",
        NavigationMode::DetailedView => "↑↓ Navigate fields | c Copy value | ESC Close",
        NavigationMode::ErrorDisplay => "ESC Close error",
    };

    let mut footer_content = vec![Line::from(Span::styled(footer_text, Style::default().fg(Color::DarkGray)))];
    
    if let Some(status) = &app.status_message {
        footer_content.insert(0, Line::from(Span::styled(status, Style::default().fg(Color::Green))));
    }

    let footer = Paragraph::new(footer_content)
        .alignment(Alignment::Center)
        .block(Block::default().borders(Borders::ALL).border_style(Style::default().fg(Color::DarkGray)));
    
    frame.render_widget(footer, area);
}