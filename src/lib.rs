use chrono::{Duration, NaiveDateTime};
use duckdb::{Connection, Result, params};

pub(crate) fn append_dates(conn: Connection) -> Result<()> {
    conn.execute("CREATE TABLE nineties (d TIMESTAMP);", params![])?;

    let mut appender = conn.appender("nineties")?;

    let start = NaiveDateTime::parse_from_str("1990-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        .expect("Invalid start date");
    let end = NaiveDateTime::parse_from_str("2000-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        .expect("Invalid end date");

    let timer = chrono::Utc::now();

    let mut current = start;
    while current < end {
        let formatted = current.format("%Y-%m-%d %H:%M:%S").to_string();
        appender.append_row(&[formatted.as_str()])?;
        current += Duration::minutes(1);
    }

    appender.flush()?;

    let runtime_in_ms = (chrono::Utc::now() - timer).num_milliseconds();
    println!("Insert took {runtime_in_ms}ms");

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM nineties;", params![], |row| {
        row.get(0)
    })?;
    println!("Inserted {} rows", count);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        assert_eq!(4, 4);
    }
}
