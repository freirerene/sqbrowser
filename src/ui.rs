use anyhow::Result;
use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, TableState},
    Frame,
};

use crate::database::{Database, QueryResult, TableInfo};

#[derive(Debug, Clone, PartialEq)]
pub enum NavigationMode {
    Table,
    Data,
    Query,
}

#[derive(Debug)]
pub struct AppState {
    pub tables: Vec<String>,
    pub selected_table_idx: usize,
    pub selected_row_idx: usize,
    pub navigation_mode: NavigationMode,
    pub current_query: Option<String>,
    pub query_input: String,
    pub data_offset: usize,
    pub page_size: usize,
    pub current_data: Option<QueryResult>,
    pub db_path: String,
    pub status_message: Option<String>,
    pub show_help: bool,
}

impl AppState {
    pub fn new(db_path: String, tables: Vec<String>) -> Self {
        Self {
            tables,
            selected_table_idx: 0,
            selected_row_idx: 0,
            navigation_mode: NavigationMode::Table,
            current_query: None,
            query_input: String::new(),
            data_offset: 0,
            page_size: 25,
            current_data: None,
            db_path,
            status_message: None,
            show_help: false,
        }
    }

    pub fn current_table(&self) -> Option<&str> {
        self.tables.get(self.selected_table_idx).map(|s| s.as_str())
    }

    pub fn handle_key_event(&mut self, key_event: KeyEvent, db: &Database) -> Result<bool> {
        match self.navigation_mode {
            NavigationMode::Query => self.handle_query_input(key_event, db),
            NavigationMode::Table => self.handle_table_navigation(key_event, db),
            NavigationMode::Data => self.handle_data_navigation(key_event, db),
        }
    }

    fn handle_query_input(&mut self, key_event: KeyEvent, db: &Database) -> Result<bool> {
        match key_event.code {
            KeyCode::Esc => {
                self.navigation_mode = NavigationMode::Data;
                self.query_input.clear();
            }
            KeyCode::Enter => {
                if !self.query_input.trim().is_empty() {
                    if let Some(table_name) = self.current_table() {
                        match db.execute_custom_query(&self.query_input, table_name, 0, self.page_size) {
                            Ok(result) => {
                                self.current_query = Some(self.query_input.clone());
                                self.current_data = Some(result);
                                self.selected_row_idx = 0;
                                self.data_offset = 0;
                                self.status_message = Some("Query executed successfully".to_string());
                            }
                            Err(e) => {
                                self.status_message = Some(format!("Query error: {}", e));
                            }
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

    fn handle_table_navigation(&mut self, key_event: KeyEvent, db: &Database) -> Result<bool> {
        match key_event.code {
            KeyCode::Up => {
                if self.selected_table_idx > 0 {
                    self.selected_table_idx -= 1;
                    self.reset_data_view();
                    self.load_current_data(db)?;
                }
            }
            KeyCode::Down => {
                if self.selected_table_idx < self.tables.len().saturating_sub(1) {
                    self.selected_table_idx += 1;
                    self.reset_data_view();
                    self.load_current_data(db)?;
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

    fn handle_data_navigation(&mut self, key_event: KeyEvent, db: &Database) -> Result<bool> {
        match key_event.code {
            KeyCode::Up => {
                if self.selected_row_idx > 0 {
                    self.selected_row_idx -= 1;
                } else if self.data_offset > 0 {
                    self.data_offset = self.data_offset.saturating_sub(self.page_size);
                    self.selected_row_idx = self.page_size - 1;
                    self.load_current_data(db)?;
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
                        self.load_current_data(db)?;
                    }
                }
            }
            KeyCode::PageUp => {
                if self.data_offset > 0 {
                    self.data_offset = self.data_offset.saturating_sub(self.page_size);
                    self.selected_row_idx = 0;
                    self.load_current_data(db)?;
                }
            }
            KeyCode::PageDown => {
                if let Some(data) = &self.current_data {
                    if self.data_offset + data.rows.len() < data.total_rows {
                        self.data_offset += self.page_size;
                        self.selected_row_idx = 0;
                        self.load_current_data(db)?;
                    }
                }
            }
            KeyCode::Home => {
                self.data_offset = 0;
                self.selected_row_idx = 0;
                self.load_current_data(db)?;
            }
            KeyCode::End => {
                if let Some(data) = &self.current_data {
                    self.data_offset = data.total_rows.saturating_sub(self.page_size);
                    self.selected_row_idx = 0;
                    self.load_current_data(db)?;
                }
            }
            KeyCode::Left => {
                self.navigation_mode = NavigationMode::Table;
                self.reset_data_view();
            }
            KeyCode::Char('i') => {
                self.navigation_mode = NavigationMode::Query;
                self.query_input.clear();
            }
            KeyCode::Char('e') => {
                self.export_to_csv(db)?;
            }
            KeyCode::Char('r') | KeyCode::Enter => {
                self.load_current_data(db)?;
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

    fn reset_data_view(&mut self) {
        self.current_query = None;
        self.current_data = None;
        self.selected_row_idx = 0;
        self.data_offset = 0;
    }

    pub fn load_current_data(&mut self, db: &Database) -> Result<()> {
        if let Some(table_name) = self.current_table() {
            let result = if let Some(query) = &self.current_query {
                db.execute_custom_query(query, table_name, self.data_offset, self.page_size)?
            } else {
                db.get_table_data(table_name, self.data_offset, self.page_size)?
            };
            self.current_data = Some(result);
        }
        Ok(())
    }

    fn export_to_csv(&mut self, db: &Database) -> Result<()> {
        if let Some(table_name) = self.current_table() {
            let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
            let filename = if let Some(query) = &self.current_query {
                format!("query_export_{}.csv", timestamp)
            } else {
                format!("{}_{}.csv", table_name, timestamp)
            };

            let rows_exported = if let Some(query) = &self.current_query {
                db.export_query_to_csv(query, &filename)?
            } else {
                db.export_table_to_csv(table_name, &filename)?
            };

            self.status_message = Some(format!("Exported {} rows to {}", rows_exported, filename));
        }
        Ok(())
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

    // Help overlay
    if app.show_help {
        render_help(frame);
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
            .title(Span::styled("Tables", title_style)));
    
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

    let border_style = if app.navigation_mode == NavigationMode::Data {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::Green)
    };

    let title_style = if app.navigation_mode == NavigationMode::Data {
        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)
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

        // Create table rows
        let rows: Vec<Row> = data.rows.iter().enumerate().map(|(i, row_data)| {
            let cells: Vec<Cell> = row_data.iter().map(|cell| {
                let content = if cell.len() > 40 {
                    format!("{}...", &cell[..37])
                } else {
                    cell.clone()
                };
                Cell::from(content)
            }).collect();

            if app.navigation_mode == NavigationMode::Data && i == app.selected_row_idx {
                Row::new(cells).style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))
            } else {
                Row::new(cells)
            }
        }).collect();

        // Create column widths
        let widths: Vec<Constraint> = data.columns.iter().map(|_| Constraint::Percentage(100 / data.columns.len().max(1) as u16)).collect();

        let table = Table::new(rows, widths)
            .header(Row::new(data.columns.iter().map(|h| Cell::from(h.as_str())).collect::<Vec<_>>())
                .style(Style::default().fg(Color::Magenta).add_modifier(Modifier::BOLD)))
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
        Line::from("  ↑↓      Navigate/scroll rows"),
        Line::from("  PgUp/Dn Page navigation"),
        Line::from("  Home    Go to first page"),
        Line::from("  End     Go to last page"),
        Line::from("  ←       Back to table list"),
        Line::from("  i       Enter query mode"),
        Line::from("  e       Export to CSV"),
        Line::from("  r/Enter Refresh data"),
        Line::from("  h       Toggle this help"),
        Line::from("  Ctrl+C  Exit application"),
        Line::from(""),
        Line::from(Span::styled("Query Mode:", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD))),
        Line::from("  Type your SQL query"),
        Line::from("  Enter   Execute query"),
        Line::from("  ESC     Cancel query"),
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
        NavigationMode::Data => "↑↓ Navigate | PgUp/Dn Page | ← Back | i Query | e Export | h Help | Ctrl+C Exit",
        NavigationMode::Query => "Type query | Enter Execute | ESC Cancel",
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