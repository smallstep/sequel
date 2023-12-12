CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;

CREATE TABLE person_test (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    deleted_at timestamptz,
    name varchar(255) NOT NULL,
    email varchar(255) NOT NULL
);

CREATE UNIQUE INDEX ON person_test(email);

CREATE TABLE array_test (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    deleted_at timestamptz,
    cidrs cidr[],
    integers integer[],
    varchars varchar(255)[],
    texts text[]
);
