use std::collections::HashMap;
use std::fs::File;
use std::error::Error;
use csv;

pub struct Column<T> {
  pub name: String,
  pub data: Vec<T>,
}

pub struct Table {
  pub columns: HashMap<String, Column<String>>
}

impl Table {
  pub fn load_csv(path: &str) -> Result<Self, Box<dyn Error>> {
    let mut reader = csv::Reader::from_path(path)?;
    let headers = reader.headers()?.clone();

    let mut columns: HashMap<String, Column<String>> = HashMap::new();

    for header in headers.iter() {
      columns.insert(header.to_string(), Column {
        name: header.to_string(),
        data: Vec::new(),
      });
    }

    for record in reader.records() {
      let record = record?;
      for  (i, field) in record.iter().enumerate() {
        let header = &headers[i];
        columns.get_mut(header).unwrap().data.push(field.to_string());
      }
    }

    Ok(Table { columns })
  }
}