#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname sequel --file /tmp/schema.sql
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname sequel -c "INSERT INTO person_test (id, name, email) VALUES ('ffb39de50af9c0ef6719e1c5698bc9ef', 'read replica user1', 'read1@replica.com'), ('2144dd61a17ab3c24d487916698bc9f5', 'read replica user2', 'read2@replica.com'), ('cc577469cc0aca5f0ed8df51698bc9ff', 'read replica user3', 'read3@replica.com');"
