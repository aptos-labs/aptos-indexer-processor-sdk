use parquet::{record::RecordWriter, schema::types::Type};
use std::sync::Arc;

pub trait ExtractResources<ParquetType> {
    fn extract(self) -> Vec<ParquetType>;
}

pub trait NamedTable {
    const TABLE_NAME: &'static str;
}

pub trait HasVersion {
    fn version(&self) -> i64;
}

pub trait HasParquetSchema {
    fn schema() -> Arc<parquet::schema::types::Type>;
}

pub trait GetTimeStamp {
    fn get_timestamp(&self) -> chrono::NaiveDateTime;
}

impl<ParquetType> HasParquetSchema for ParquetType
where
    ParquetType: std::fmt::Debug + Default + Sync + Send,
    for<'a> &'a [ParquetType]: RecordWriter<ParquetType>,
{
    fn schema() -> Arc<Type> {
        let example: Self = Default::default();
        [example].as_slice().schema().unwrap()
    }
}
