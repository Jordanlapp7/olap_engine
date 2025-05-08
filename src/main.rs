mod table;
mod plan;
mod execution;
mod types;
mod util;

use crate::table::Table;

fn main() {
    println!("OLAP Engine Initialized.");

    let path = "data/sample.csv";

    match Table::load_csv(path) {
        Ok(table) => {
            for (col_name, column) in &table.columns {
                println!("Column: {}", col_name);
                for value in &column.data {
                    println!("  {}", value);
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to load CSV: {}", e);
        }
    }

    // TODO: accept CLI args to load CSV and run queries
}
