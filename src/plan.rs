use crate::execution::DataChunk;

pub trait Executable {
  fn execute(&self) -> DataChunk;
}