import os
import sqlite3
import sys

import keyboard
from rich.align import Align
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table


def get_tables_from_db(db_path):
    """Get all table names from SQLite database"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tables
    except sqlite3.Error as e:
        print(f"Error accessing database: {e}")
        return []


def get_table_data(db_path, table_name, query=None, offset=0, limit=25):
    """Get data from a specific table with pagination"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        if query:
            # For custom queries, add LIMIT and OFFSET
            paginated_query = f"{query} LIMIT {limit} OFFSET {offset}"
            cursor.execute(paginated_query)
            data = cursor.fetchall()

            # Get total count for custom query (this is a simplified approach)
            try:
                count_query = f"SELECT COUNT(*) FROM ({query})"
                cursor.execute(count_query)
                total_rows = cursor.fetchone()[0]
            except:  # noqa: E722
                # If count fails, estimate based on current data
                total_rows = offset + len(data) + (limit if len(data) == limit else 0)
        else:
            # For regular table queries
            cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset}")
            data = cursor.fetchall()

            # Get total count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_rows = cursor.fetchone()[0]

        # Get column names
        if query:
            # Execute original query to get column info
            original_query = query + " LIMIT 1"
            cursor.execute(original_query)
            columns = [description[0] for description in cursor.description]
        else:
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in cursor.fetchall()]

        conn.close()
        return columns, data, total_rows
    except sqlite3.Error as e:
        print(f"Error reading table {table_name}: {e}")
        return [], [], 0


def create_sidebar(tables, selected_table_idx, navigation_mode):
    """Create sidebar with table list"""
    table_list = Table.grid()
    table_list.add_column(justify="left", style="cyan")

    for i, table in enumerate(tables):
        if i == selected_table_idx:
            if navigation_mode == "table":
                table_list.add_row(f"[bold yellow]▶ {table}[/bold yellow]")
            else:
                table_list.add_row(f"[dim]▶ {table}[/dim]")
        else:
            table_list.add_row(f"  {table}")

    if not tables:
        table_list.add_row("[dim]No tables found[/dim]")

    title_style = "cyan" if navigation_mode == "table" else "dim"
    return Panel(
        table_list,
        title=f"[bold {title_style}]Tables[/bold {title_style}]",
        border_style=title_style,
        height=None,
    )


def create_main_area(
    db_path,
    tables,
    selected_table_idx,
    navigation_mode,
    selected_row_idx,
    current_query=None,
    data_offset=0,
):
    """Create main area with table data"""
    if not tables or selected_table_idx >= len(tables):
        return Panel(
            "[dim]Select a table to view its contents[/dim]",
            title="[bold green]Table Contents[/bold green]",
            border_style="green",
        )

    table_name = tables[selected_table_idx]
    page_size = 25  # Rows per page
    columns, data, total_rows = get_table_data(
        db_path, table_name, current_query, data_offset, page_size
    )

    if not columns:
        return Panel(
            f"[red]Error reading table '{table_name}'[/red]",
            title="[bold green]Table Contents[/bold green]",
            border_style="green",
        )

    # Create rich table
    rich_table = Table(show_header=True, header_style="bold magenta")

    # Add columns
    for col in columns:
        rich_table.add_column(col, style="cyan")

    # Add rows with navigation highlighting
    for i, row in enumerate(data):
        # Convert all values to strings and truncate if too long
        row_data = []
        for val in row:
            str_val = str(val) if val is not None else "NULL"
            if len(str_val) > 40:
                str_val = str_val[:37] + "..."
            row_data.append(str_val)

        # Highlight selected row in data navigation mode
        if navigation_mode == "data" and i == selected_row_idx:
            row_data = [f"[bold yellow]{val}[/bold yellow]" for val in row_data]

        rich_table.add_row(*row_data)

    # Calculate pagination info
    current_page = (data_offset // page_size) + 1
    total_pages = (total_rows + page_size - 1) // page_size if total_rows > 0 else 1
    start_row = data_offset + 1 if total_rows > 0 else 0
    end_row = min(data_offset + len(data), total_rows)

    info_text = (
        f"Table: {table_name} | Total: {total_rows} rows | Columns: {len(columns)}"
    )
    if total_pages > 1:
        info_text += (
            f" | Page {current_page}/{total_pages} | Rows {start_row}-{end_row}"
        )

    if current_query:
        info_text += " | Custom Query"

    border_style = "yellow" if navigation_mode == "data" else "green"
    title_color = "yellow" if navigation_mode == "data" else "green"

    return Panel(
        rich_table,
        title=f"[bold {title_color}]{info_text}[/bold {title_color}]",
        border_style=border_style,
    )


def create_layout(
    db_path,
    tables,
    selected_table_idx,
    navigation_mode,
    selected_row_idx,
    current_query=None,
    query_mode=False,
    query_input="",
    data_offset=0,
):
    """Create the complete layout"""

    # Create layout with sidebar and main area
    layout = Layout()

    # Split into header, body, and footer
    if query_mode:
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body"),
            Layout(name="query", size=3),
            Layout(name="footer", size=3),
        )
    else:
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body"),
            Layout(name="footer", size=3),
        )

    # Split body into sidebar and main
    layout["body"].split_row(Layout(name="sidebar", size=25), Layout(name="main"))

    # Add content
    layout["header"].update(
        Panel(
            Align(
                f"[bold green]SQLite Browser - {os.path.basename(db_path)}[/bold green]",
                align="center",
            ),
            border_style="green",
        )
    )

    layout["sidebar"].update(
        create_sidebar(tables, selected_table_idx, navigation_mode)
    )
    layout["main"].update(
        create_main_area(
            db_path,
            tables,
            selected_table_idx,
            navigation_mode,
            selected_row_idx,
            current_query,
            data_offset,
        )
    )

    if query_mode:
        layout["query"].update(
            Panel(
                f"[bold cyan]Query: [/bold cyan]{query_input}█",
                title="[bold cyan]Enter SQL Query (ESC to cancel)[/bold cyan]",
                border_style="cyan",
            )
        )

    # Dynamic footer based on navigation mode
    if navigation_mode == "table":
        footer_text = "[dim]↑↓ Navigate tables | → Enter table | Enter to refresh | Ctrl+C to exit[/dim]"
    elif navigation_mode == "data":
        footer_text = "[dim]↑↓ Navigate/Scroll | PgUp/PgDn Page | ← Back | i Query | Home/End | Ctrl+C exit[/dim]"

    layout["footer"].update(
        Panel(Align(footer_text, align="center"), border_style="dim")
    )

    return layout


def execute_query(db_path, table_name, query):
    """Execute a custom query on the database"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Add table name context if query doesn't contain FROM
        if "FROM" not in query.upper() and "from" not in query:
            query = f"{query} FROM {table_name}"

        cursor.execute(query)
        data = cursor.fetchall()
        columns = [description[0] for description in cursor.description]

        conn.close()
        return columns, data, None
    except sqlite3.Error as e:
        return [], [], str(e)


def main():
    if len(sys.argv) != 2:
        print("Usage: python sqlite_browser_paginated.py <database_path>")
        sys.exit(1)

    db_path = sys.argv[1]

    if not os.path.exists(db_path):
        print(f"Error: Database file '{db_path}' not found")
        sys.exit(1)

    console = Console()

    # Get tables from database
    tables = get_tables_from_db(db_path)
    selected_table_idx = 0
    selected_row_idx = 0
    navigation_mode = "table"  # "table" or "data"
    current_query = None
    query_mode = False
    query_input = ""
    data_offset = 0  # For pagination
    page_size = 25

    if not tables:
        print("No tables found in database")
        sys.exit(1)

    # Set terminal to prevent echoing
    os.system("stty -echo")

    try:
        # Clear screen and hide cursor for full screen experience
        console.clear()
        with console.screen(hide_cursor=True):
            with Live(
                create_layout(
                    db_path,
                    tables,
                    selected_table_idx,
                    navigation_mode,
                    selected_row_idx,
                    current_query,
                    query_mode,
                    query_input,
                    data_offset,
                ),
                refresh_per_second=10,
                console=console,
                screen=True,
            ) as live:
                try:
                    while True:
                        event = keyboard.read_event()
                        if event.event_type == keyboard.KEY_DOWN:
                            old_table_idx = selected_table_idx
                            old_row_idx = selected_row_idx
                            old_navigation_mode = navigation_mode
                            old_query_mode = query_mode
                            old_query_input = query_input
                            old_data_offset = data_offset

                            if query_mode:
                                # Handle query input mode
                                if event.name == "esc":
                                    query_mode = False
                                    query_input = ""
                                elif event.name == "enter":
                                    if query_input.strip():
                                        table_name = tables[selected_table_idx]
                                        columns, data, error = execute_query(
                                            db_path, table_name, query_input
                                        )
                                        if not error:
                                            current_query = query_input
                                            selected_row_idx = 0
                                            data_offset = 0
                                        query_mode = False
                                        query_input = ""
                                elif event.name == "backspace":
                                    if query_input:
                                        query_input = query_input[:-1]
                                elif event.name == "space":
                                    query_input += " "
                                elif len(event.name) == 1:  # Regular character
                                    query_input += event.name

                            elif navigation_mode == "table":
                                # Table navigation mode
                                if event.name == "up":
                                    if selected_table_idx > 0:
                                        selected_table_idx -= 1
                                        selected_row_idx = 0
                                        current_query = None
                                        data_offset = 0
                                elif event.name == "down":
                                    if selected_table_idx < len(tables) - 1:
                                        selected_table_idx += 1
                                        selected_row_idx = 0
                                        current_query = None
                                        data_offset = 0
                                elif event.name == "right":
                                    navigation_mode = "data"
                                    selected_row_idx = 0
                                    data_offset = 0
                                elif event.name == "enter":
                                    # Refresh the current table display
                                    current_query = None
                                    selected_row_idx = 0
                                    data_offset = 0
                                elif event.name == "ctrl+c" or event.name == "esc":
                                    break

                            elif navigation_mode == "data":
                                # Data navigation mode with pagination
                                table_name = tables[selected_table_idx]
                                columns, data, total_rows = get_table_data(
                                    db_path,
                                    table_name,
                                    current_query,
                                    data_offset,
                                    page_size,
                                )
                                current_page_rows = len(data)

                                if event.name == "up":
                                    if selected_row_idx > 0:
                                        selected_row_idx -= 1
                                    elif data_offset > 0:
                                        # Scroll up to previous page
                                        data_offset = max(0, data_offset - page_size)
                                        selected_row_idx = page_size - 1
                                        # Adjust row index if new page has fewer rows
                                        new_columns, new_data, _ = get_table_data(
                                            db_path,
                                            table_name,
                                            current_query,
                                            data_offset,
                                            page_size,
                                        )
                                        if selected_row_idx >= len(new_data):
                                            selected_row_idx = max(0, len(new_data) - 1)
                                elif event.name == "down":
                                    if selected_row_idx < current_page_rows - 1:
                                        selected_row_idx += 1
                                    elif data_offset + current_page_rows < total_rows:
                                        # Scroll down to next page
                                        data_offset += page_size
                                        selected_row_idx = 0
                                elif event.name == "page up":
                                    # Page up
                                    if data_offset > 0:
                                        data_offset = max(0, data_offset - page_size)
                                        selected_row_idx = 0
                                elif event.name == "page down":
                                    # Page down
                                    if data_offset + current_page_rows < total_rows:
                                        data_offset = min(
                                            total_rows - page_size,
                                            data_offset + page_size,
                                        )
                                        if data_offset < 0:
                                            data_offset = 0
                                        selected_row_idx = 0
                                elif event.name == "home":
                                    # Go to first page
                                    data_offset = 0
                                    selected_row_idx = 0
                                elif event.name == "end":
                                    # Go to last page
                                    data_offset = max(0, total_rows - page_size)
                                    selected_row_idx = 0
                                elif event.name == "left":
                                    navigation_mode = "table"
                                    selected_row_idx = 0
                                    data_offset = 0
                                elif event.name == "i":
                                    query_mode = True
                                    query_input = ""
                                elif event.name == "enter":
                                    # Refresh the current table display
                                    selected_row_idx = 0
                                    data_offset = 0
                                elif event.name == "ctrl+c" or event.name == "esc":
                                    break

                            # Update display if anything changed
                            if (
                                old_table_idx != selected_table_idx
                                or old_row_idx != selected_row_idx
                                or old_navigation_mode != navigation_mode
                                or old_query_mode != query_mode
                                or old_query_input != query_input
                                or old_data_offset != data_offset
                                or event.name == "enter"
                            ):
                                live.update(
                                    create_layout(
                                        db_path,
                                        tables,
                                        selected_table_idx,
                                        navigation_mode,
                                        selected_row_idx,
                                        current_query,
                                        query_mode,
                                        query_input,
                                        data_offset,
                                    )
                                )
                except KeyboardInterrupt:
                    pass
    finally:
        # Restore terminal echo
        os.system("stty echo")

    console.print("\n[yellow]Goodbye![/yellow]")


if __name__ == "__main__":
    main()
