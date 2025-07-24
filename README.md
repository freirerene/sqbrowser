# SqBrowser

Simple SQLite and tables browser.

It can browse SQLite databases, parquet/excel/csv files on the terminal.

**Features**:
  * query the database and the files (as if they were a sql database);
  * edit tabled files and save; edit sqlite tables and export to csv (no save to database yet);
  * create new rows;
  * create new columns with mathematical operations between other columns

**Instructions**
Configure the colors as in the `config.json` and put it in `~/.config/sqbrowser` (examples in the files `config_dark.json` and `config_light.json`).

Simple straightforward compilation: `cargo build` will create the neat little binary at `target/debug/sqbrowser`.Then run `target/debug/sqbrowser <file>`.

Navigation and manipulations are explained on the screen (also `h` for help) and are pretty intuitive, for example, to create a new column with mathematical expressions (similar to sheets programs like excel): `=` and the syntax `column_name=expression`.
