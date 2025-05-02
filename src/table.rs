use std::collections::HashMap;

pub struct Column<T> {
  pub name: String,
  pub data: Vec<T>,
}

pub struct Table {
  pub columns: HashMap<String, Column<String>>
}

impl Table {
  pub fn load_csv(_path: &str) -> Self {
    // TODO: implement CSV parsing
    Table {
      columns: HashMap::new()
    }
  }
}