import fileinput
import logging
from collections import OrderedDict

import duckdb
import sqlglot
import sqlglot.expressions

from helpers import get_sql_block, remove_schema

DEBUG = False

logging.basicConfig(level=(logging.DEBUG if DEBUG else logging.INFO))


def convert(value: str, dtype: sqlglot.expressions.DataType.Type):
    if value == "\\N":
        return None

    if dtype.this == sqlglot.expressions.DataType.Type.ARRAY:
        return value[1:-1].split(",")

    return value


def get_typed_values(row, schema):
    return tuple(convert(value=value, dtype=schema[key]) for key, value in row.items())


def get_typed_insert_query(table, values, columns):
    return sqlglot.insert(
        into=table,
        columns=columns,
        expression=sqlglot.expressions.values([*values]),
    ).sql(
        dialect="duckdb",
    )


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


def handle_copy(
    statement: sqlglot.Expression, stream, connection, table, schema, chunks: int
):
    columns = [i.this for i in statement.this.expressions]
    entries = []
    connection.sql("begin;")

    line = next(stream).rstrip()

    while line != r"\.":
        row = OrderedDict(zip(columns, line.rstrip().split("\t"), strict=False))
        values = get_typed_values(row, schema)
        entries.append(values)

        if len(entries) == chunks:
            query = get_typed_insert_query(table=table, values=entries, columns=columns)
            logging.debug(query)
            connection.sql(query)
            entries = []

        line = next(stream).rstrip()

    if entries:
        query = get_typed_insert_query(table=table, values=entries, columns=columns)
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
    connection = duckdb.connect()
    chunks = 100000
    TABLE_REGISTRY = {}

    with fileinput.input(encoding="utf-8") as f:
        for statement in sql_parsing_iterator(f):
            if statement.key == "create":
                table, schema = handle_create(statement, connection)
                TABLE_REGISTRY[table] = schema
            elif statement.key == "copy":
                table = statement.this.this.this.this
                schema = TABLE_REGISTRY[table]
                handle_copy(statement, f, connection, table, schema, chunks=chunks)

    for table in TABLE_REGISTRY.keys():
        connection.sql(f"from {table}").write_parquet(f"{table}.parquet")

    connection.close()


if __name__ == "__main__":
    start()
