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
    // Aggregate, Join, etc. will go here later
}

impl<'a> Executable for PlanNode<'a> {
    fn execute(&self) -> DataChunk {
        match self {
            PlanNode::Scan(scan) => scan.execute(),
            PlanNode::Project(proj) => proj.execute(),
            PlanNode::Filter(filt) => filt.execute(),
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
