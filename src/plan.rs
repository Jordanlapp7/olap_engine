use crate::execution::DataChunk;
use crate::table::Table;
use std::collections::HashMap;

// Trait for executable plan nodes
pub trait Executable {
    fn execute(&self) -> DataChunk;
}

// Logical plan node enum
pub enum PlanNode<'a> {
    Scan(ScanNode<'a>),
    Project(ProjectNode<'a>),
    Filter(FilterNode<'a>),
    Aggregate(AggregateNode<'a>),
}

impl<'a> Executable for PlanNode<'a> {
    fn execute(&self) -> DataChunk {
        match self {
            PlanNode::Scan(scan) => scan.execute(),
            PlanNode::Project(proj) => proj.execute(),
            PlanNode::Filter(filt) => filt.execute(),
            PlanNode::Aggregate(agg) => agg.execute(),
        }
    }
}

// Plan Node: Scan (reads full table)
pub struct ScanNode<'a> {
    pub table: &'a Table,
}

impl<'a> Executable for ScanNode<'a> {
    fn execute(&self) -> DataChunk {
        self.table
            .columns
            .iter()
            .map(|(name, col)| (name.clone(), col.data.clone()))
            .collect()
    }
}

// Plan Node: Project (select subset of columns)
pub struct ProjectNode<'a> {
    pub input: Box<PlanNode<'a>>,
    pub columns: Vec<String>,
}

impl<'a> Executable for ProjectNode<'a> {
    fn execute(&self) -> DataChunk {
        let input_chunk = self.input.execute();
        input_chunk
            .into_iter()
            .filter(|(col, _)| self.columns.contains(col))
            .collect()
    }
}

// Plan Node: Filter (apply condition to rows)
pub struct FilterNode<'a> {
    pub input: Box<PlanNode<'a>>,
    pub predicate: Box<dyn Fn(&HashMap<String, String>) -> bool + 'a>,
}

impl<'a> Executable for FilterNode<'a> {
    fn execute(&self) -> DataChunk {
        let input_chunk = self.input.execute();
        let num_rows = input_chunk.values().next().map_or(0, |v| v.len());

        // Construct row-wise view to apply predicate
        let mut results: HashMap<String, Vec<String>> = input_chunk
            .keys()
            .map(|col| (col.clone(), Vec::new()))
            .collect();

        for i in 0..num_rows {
            let row: HashMap<String, String> = input_chunk
                .iter()
                .map(|(k, v)| (k.clone(), v[i].clone()))
                .collect();

            if (self.predicate)(&row) {
                for (col, vec) in &mut results {
                    vec.push(input_chunk[col][i].clone());
                }
            }
        }

        results
    }
}

// Aggregate Function Enum
pub enum AggregateFunction {
  Count,
  Sum,
}

// Plan Node: Aggregate (group by + aggregation)
pub struct AggregateNode<'a> {
  pub input: Box<PlanNode<'a>>,
  pub group_by: Vec<String>,
  pub aggregates: Vec<(String, AggregateFunction)>,
}

impl<'a> Executable for AggregateNode<'a> {
  fn execute(&self) -> DataChunk {
      let input_chunk = self.input.execute();
      let num_rows = input_chunk.values().next().map_or(0, |v| v.len());

      let mut groups: HashMap<Vec<String>, HashMap<String, f64>> = HashMap::new();

      for i in 0..num_rows {
          let group_key: Vec<String> = self
              .group_by
              .iter()
              .map(|col| input_chunk[col][i].clone())
              .collect();

          let entry = groups.entry(group_key).or_insert_with(|| {
              let mut init = HashMap::new();
              for (col, func) in &self.aggregates {
                  match func {
                      AggregateFunction::Count => {
                          init.insert(col.clone(), 0.0);
                      }
                      AggregateFunction::Sum => {
                          init.insert(col.clone(), 0.0);
                      }
                  }
              }
              init
          });

          for (col, func) in &self.aggregates {
              let val = &input_chunk[col][i];
              match func {
                  AggregateFunction::Count => {
                      *entry.get_mut(col).unwrap() += 1.0;
                  }
                  AggregateFunction::Sum => {
                      if let Ok(v) = val.parse::<f64>() {
                          *entry.get_mut(col).unwrap() += v;
                      }
                  }
              }
          }
      }

      let mut result: DataChunk = HashMap::new();

      for col in &self.group_by {
          result.insert(col.clone(), Vec::new());
      }
      for (col, _) in &self.aggregates {
          result.insert(col.clone(), Vec::new());
      }

      for (key, agg_vals) in groups {
          for (i, col) in self.group_by.iter().enumerate() {
              result.get_mut(col).unwrap().push(key[i].clone());
          }
          for (col, val) in agg_vals {
              result.get_mut(&col).unwrap().push(val.to_string());
          }
      }

      result
  }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{Table, Column};

    fn sample_table() -> Table {
        Table {
            columns: HashMap::from([
                ("region".to_string(), Column { data: vec!["East".to_string(), "West".to_string(), "East".to_string()] }),
                ("sales".to_string(), Column { data: vec!["100".to_string(), "200".to_string(), "300".to_string()] }),
            ]),
        }
    }

    #[test]
    fn test_scan_node() {
        let table = sample_table();
        let scan = ScanNode { table: &table };
        let output = scan.execute();

        assert_eq!(output["region"], vec!["East", "West", "East"]);
        assert_eq!(output["sales"], vec!["100", "200", "300"]);
    }

    #[test]
    fn test_project_node() {
        let table = sample_table();
        let scan = PlanNode::Scan(ScanNode { table: &table });
        let project = ProjectNode {
            input: Box::new(scan),
            columns: vec!["sales".to_string()],
        };
        let output = project.execute();

        assert_eq!(output.len(), 1);
        assert_eq!(output["sales"], vec!["100", "200", "300"]);
    }

    #[test]
    fn test_filter_node() {
        let table = sample_table();
        let scan = PlanNode::Scan(ScanNode { table: &table });
        let filter = FilterNode {
            input: Box::new(scan),
            predicate: Box::new(|row| row["region"] == "East"),
        };
        let output = filter.execute();

        assert_eq!(output["region"], vec!["East", "East"]);
        assert_eq!(output["sales"], vec!["100", "300"]);
    }

    #[test]
    fn test_aggregate_node_sum() {
        let table = sample_table();
        let scan = PlanNode::Scan(ScanNode { table: &table });
        let aggregate = AggregateNode {
            input: Box::new(scan),
            group_by: vec!["region".to_string()],
            aggregates: vec![("sales".to_string(), AggregateFunction::Sum)],
        };
        let output = aggregate.execute();

        assert_eq!(output["region"].len(), 2);
        assert!(output["region"].contains(&"East".to_string()));
        assert!(output["region"].contains(&"West".to_string()));

        let east_index = output["region"].iter().position(|r| r == "East").unwrap();
        let west_index = output["region"].iter().position(|r| r == "West").unwrap();

        assert_eq!(output["sales"][east_index], "400");
        assert_eq!(output["sales"][west_index], "200");
    }

    #[test]
    fn test_aggregate_node_count() {
        let table = sample_table();
        let scan = PlanNode::Scan(ScanNode { table: &table });
        let aggregate = AggregateNode {
            input: Box::new(scan),
            group_by: vec!["region".to_string()],
            aggregates: vec![("sales".to_string(), AggregateFunction::Count)],
        };
        let output = aggregate.execute();

        assert_eq!(output["region"].len(), 2);
        assert!(output["region"].contains(&"East".to_string()));
        assert!(output["region"].contains(&"West".to_string()));

        let east_index = output["region"].iter().position(|r| r == "East").unwrap();
        let west_index = output["region"].iter().position(|r| r == "West").unwrap();

        assert_eq!(output["sales"][east_index], "2");
        assert_eq!(output["sales"][west_index], "1");
    }
}