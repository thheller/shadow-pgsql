# shadow-pgsql

WARNING: Alpha-Level Software. DO NOT USE YET! I don't.

## A PostgreSQL Client for the JVM.

The standard interface in Java to talk to Postgres is the official JDBC-Driver. JDBC tries to
create a common Interface for Databases but not all SQL Databases are created equal. That
makes using Postgres-specific features rather cumbersome to use.

This library tries to provide a far simpler yet more powerful interface to the PostgreSQL Server.

### Requirements

JDK 8+, PostgreSQL 9+ (only tested against 9.1.2, 9.3.5, might work with older versions)

### Status

- TCP Procotol is implemented and working.
- Only a handful of [Types](https://github.com/thheller/shadow-pgsql/tree/master/src/java/shadow/pgsql/types) have been implemented. Not enough for basically anything.
- API has somewhat stabilized (after 10 rewrites)

### TODO:

- more Types
- better Error Handling & Recovery (none right now)
- Docs
- SSL & Auth Support
- Generics (didn't write any serious Java for over 10 years, need to learn Generics first)
- check Performance (should be faster than JDBC, but needs check to be sure)


## Usage

```java
try (Connection pg = Database.setup("localhost", 5432, "zilence", "shadow_pgsql")
                             .connect()) {
  // basic INSERT
  long rows = pg.execute("INSERT INTO num_types (fint4) VALUES ($1)", 1);
  
  // INSERT with SERIAL id, returns generated id
  SimpleQuery query = new SimpleQuery(
    "INSERT INTO num_types (fint4) VALUES ($1) RETURNING id"
  );
  
  query.setResultBuilder(Handlers.SINGLE_ROW);
  query.setRowBuilder(Handlers.SINGLE_COLUMN);
  
  int insertedId = (int) pg.executeQuery(query, 1);
  
  // SELECT
  List numTypes = (List) pg.executeQuery("SELECT * FROM num_types");
}

```

Clojure will follow.

## License

Copyright © 2014 Thomas Heller

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
