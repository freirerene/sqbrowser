mod database;
mod file_reader;
mod data_source;
mod ui;

use anyhow::{Context, Result};
use clap::Parser;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    Terminal,
};
use std::{
    io,
    path::PathBuf,
    time::{Duration, Instant},
};

use data_source::DataSource;
use ui::{AppState, NavigationMode, render_ui};

#[derive(Parser)]
#[command(name = "sqbrowser")]
#[command(about = "A file browser supporting SQLite databases, CSV, XLSX, and Parquet files")]
struct Args {
    /// Path to the file (SQLite database, CSV, XLSX, or Parquet)
    file: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Verify file exists
    if !args.file.exists() {
        return Err(anyhow::anyhow!("File '{}' not found", args.file.display()));
    }

    // Open data source
    let data_source = DataSource::open(args.file.clone())
        .context("Failed to open file")?;

    // Get tables/sheets
    let tables = data_source.get_tables()
        .context("Failed to get table/sheet list from file")?;

    if tables.is_empty() {
        return Err(anyhow::anyhow!("No tables/sheets found in file"));
    }

    // Initialize app state
    let mut app = AppState::new(
        args.file.to_string_lossy().to_string(),
        tables
    );

    // Load initial data
    app.load_current_data(&data_source)?;

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Run the application
    let result = run_app(&mut terminal, &mut app, &data_source);

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = result {
        eprintln!("Application error: {}", err);
        return Err(err);
    }

    Ok(())
}

fn run_app<B: ratatui::backend::Backend>(
    terminal: &mut Terminal<B>,
    app: &mut AppState,
    data_source: &DataSource,
) -> Result<()> {
    let mut last_tick = Instant::now();
    let tick_rate = Duration::from_millis(100);

    loop {
        // Draw UI
        terminal.draw(|f| render_ui(f, app))?;

        // Handle events
        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));

        if crossterm::event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                // Clear status message on any key press
                if app.status_message.is_some() {
                    app.status_message = None;
                }

                // Handle key event
                if !app.handle_key_event(key, data_source)? {
                    return Ok(());
                }

                // Load data if we're in data mode and don't have current data
                if app.navigation_mode == NavigationMode::Data && app.current_data.is_none() {
                    app.load_current_data(data_source)?;
                }
            }
        }

        if last_tick.elapsed() >= tick_rate {
            last_tick = Instant::now();
        }
    }
}