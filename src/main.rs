mod database;
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

use database::Database;
use ui::{AppState, NavigationMode, render_ui};

#[derive(Parser)]
#[command(name = "sqbrowser")]
#[command(about = "A simple SQLite database browser with TUI interface")]
struct Args {
    /// Path to the SQLite database file
    database: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Verify database file exists
    if !args.database.exists() {
        return Err(anyhow::anyhow!("Database file '{}' not found", args.database.display()));
    }

    // Open database
    let db = Database::open(&args.database)
        .context("Failed to open database")?;

    // Get tables
    let tables = db.get_tables()
        .context("Failed to get table list from database")?;

    if tables.is_empty() {
        return Err(anyhow::anyhow!("No tables found in database"));
    }

    // Initialize app state
    let mut app = AppState::new(
        args.database.to_string_lossy().to_string(),
        tables
    );

    // Load initial data
    app.load_current_data(&db)?;

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Run the application
    let result = run_app(&mut terminal, &mut app, &db);

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
    db: &Database,
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
                if !app.handle_key_event(key, db)? {
                    return Ok(());
                }

                // Load data if we're in data mode and don't have current data
                if app.navigation_mode == NavigationMode::Data && app.current_data.is_none() {
                    app.load_current_data(db)?;
                }
            }
        }

        if last_tick.elapsed() >= tick_rate {
            last_tick = Instant::now();
        }
    }
}