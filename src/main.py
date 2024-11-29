import fileinput
import logging
import pathlib
from collections import OrderedDict

import duckdb
import environ
import sqlglot
import sqlglot.expressions

from helpers import get_sql_block, remove_schema

env = environ.Env()
BASE_DIR = pathlib.Path(__file__).parent.parent
environ.Env.read_env(str(BASE_DIR / ".env"))

DEBUG = env.bool("DEBUG", default=False)

logging.basicConfig(level=(logging.DEBUG if DEBUG else logging.INFO))


def get_typed_values(row, schema):
    # TODO: handle complex data types
    return tuple(v if v != "\\N" else None for v in row.values())


def get_typed_insert_query(table, row, schema):
    return sqlglot.insert(
        into=table,
        dialect="duckdb",
        columns=schema.keys(),
        expression=sqlglot.expressions.values([get_typed_values(row, schema)]),
    ).sql()


def handle_create(statement, connection):
    create_query = remove_schema(statement)
    table_name = create_query.this.this.this.this
    connection.sql(create_query.sql(dialect="duckdb"))

    logging.debug(repr(create_query))

    schema = OrderedDict(
        (exp.this.this, exp.kind) for exp in create_query.this.expressions
    )
    logging.debug(schema)
    return table_name, schema


def handle_copy(statement, stream, connection, table, schema):
    columns = [i.this for i in statement.this.expressions]
    connection.sql("begin;")

    # TODO: bulk insert!
    for line in stream:
        if line.rstrip() == r"\.":
            break

        row = OrderedDict(zip(columns, line.rstrip().split("\t"), strict=False))
        logging.debug(row)

        query = get_typed_insert_query(table=table, row=row, schema=schema)
        logging.debug(query)
        connection.sql(query)

    connection.sql("commit;")


def sql_parsing_iterator(stream):
    for _block in get_sql_block(stream):
        parsed = sqlglot.parse_one(_block)

        if parsed.key in [
            "create",
            "copy",
        ]:
            yield parsed


def start() -> None:
    # TODO: parametrize this!
    connection = duckdb.connect("dump.db")

    TABLE_REGISTRY = {}

    with fileinput.input(encoding="utf-8") as f:
        for statement in sql_parsing_iterator(f):
            if statement.key == "create":
                table, schema = handle_create(statement, connection)
                TABLE_REGISTRY[table] = schema
            elif statement.key == "copy":
                print(repr(statement.this.this.this.this))
                table = statement.this.this.this.this
                schema = TABLE_REGISTRY[table]
                handle_copy(statement, f, connection, table, schema)

    connection.close()


if __name__ == "__main__":
    start()
