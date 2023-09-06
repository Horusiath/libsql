use libsql::Database;

#[tokio::main]
async fn main() {
    let db = if let Ok(url) = std::env::var("LIBSQL_HRANA_URL") {
        let token = std::env::var("TURSO_AUTH_TOKEN").unwrap_or_else(|_| {
            println!("TURSO_AUTH_TOKEN not set, using empty token...");
            "".to_string()
        });

        Database::open_with_sync("db.sqld", url, token)
            .await
            .unwrap()
    } else {
        Database::open_in_memory().unwrap()
    };

    let conn = db.connect().await.unwrap();

    conn.execute("CREATE TABLE IF NOT EXISTS users (email TEXT)", ())
        .await
        .unwrap();

    let stmt = conn
        .prepare("INSERT INTO users (email) VALUES (?1)")
        .await
        .unwrap();

    stmt.execute(&libsql::params!["foo@example.com"])
        .await
        .unwrap();

    db.sync().await.unwrap();

    let stmt = conn
        .prepare("SELECT * FROM users WHERE email = ?1")
        .await
        .unwrap();

    let mut rows = stmt
        .query(&libsql::params!["foo@example.com"])
        .await
        .unwrap();

    let row = rows.next().unwrap().unwrap();

    let value = row.get_value(0).unwrap();

    println!("Row: {:?}", value);
}
