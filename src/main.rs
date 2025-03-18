use duckdb::Connection;
use duckdb::Result;
use lib::append_dates;

mod lib;

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;

    append_dates(conn)?;

    Ok(())
}
