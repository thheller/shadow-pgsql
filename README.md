# shadow-pgsql

WARNING: Beta-Level Software, use at your own risk. I'm using it in production for a while now and successfully executed a couple million queries.

```
[thheller/shadow-pgsql "0.8.1"]
```

I need to write some docs ...

## A PostgreSQL Client for the JVM.

The standard interface in Java to talk to Postgres is the official JDBC-Driver. JDBC tries to
create a common Interface for Databases but not all SQL Databases are created equal. That
makes using Postgres-specific features rather cumbersome to use.

This library tries to provide a far simpler yet more powerful interface to the PostgreSQL Server.

### Requirements

JDK 8+, PostgreSQL 9+ (only tested against 9.1.2, 9.3.5, might work with older versions)

### Features

- Fully customizable Types via simple [Interface](https://github.com/thheller/shadow-pgsql/blob/master/src/java/shadow/pgsql/TypeHandler.java)
- Binary Format when supported by Type

### Status

- [Basic Types](https://github.com/thheller/shadow-pgsql/tree/master/src/java/shadow/pgsql/types) have been implemented.
- Query & Statement support
- Transactions with Savepoints

### TODO:

- more Types
- UTF-8 is hardcoded right now, things will go wrong if backend is not UTF-8
- Tests
- better Errors (Messages)
- Docs
- Auth Support (MD5, ...)
- Generics (didn't write any serious Java for over 10 years, need to learn Generics first)
- Cursor Support
- FunctionCall API
- Copy API

## Usage

Needs more docs, for now check what can hardly be called Tests.

- [Java](https://github.com/thheller/shadow-pgsql/blob/master/test/shadow/pgsql/BasicTest.java)
- [Clojure](https://github.com/thheller/shadow-pgsql/blob/master/test/shadow/pgsql_test.clj)

## License

Copyright © 2014 Thomas Heller

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
