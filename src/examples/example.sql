
CREATE TABLE examples (
  id serial not null,

  some_text text,
  some_bool bool,
  some_int int4,

  kw text,
  edn text,
  primary key(id)
);

CREATE TABLE users (
  id serial not null,
  login text not null,
  password_hash bytea not null,
  created_at timestamp not null,
  primary key(id)
);

CREATE TABLE projects (
    id serial not null,
    name text not null,
    tags text[] not null,
    PRIMARY KEY(id)
);