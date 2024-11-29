import argparse
import fileinput
import logging
import pathlib
from collections import OrderedDict

import duckdb
import pyarrow as pa
import sqlglot
import sqlglot.expressions
from tqdm import tqdm

from helpers import get_sql_block, remove_schema

DEBUG = False


def convert(value: str, dtype: sqlglot.expressions.DataType.Type):
    if value == "\\N":
        return None

    if dtype.this == sqlglot.expressions.DataType.Type.ARRAY:
        return value[1:-1].split(",")

    return value


def get_typed_values(row, schema):
    return OrderedDict(
        (key, convert(value=value, dtype=schema[key])) for key, value in row.items()
    )


def get_typed_insert_query(table, values, columns):
    return sqlglot.insert(
        into=table,
        columns=columns,
        expression=sqlglot.expressions.values([*values]),
    ).sql(
        dialect="duckdb",
    )


def handle_create(statement, db):
    create_query = remove_schema(statement)
    table_name = create_query.this.this.this.this
    with duckdb.connect(db) as connection:
        connection.sql(create_query.sql(dialect="duckdb"))

    logging.debug(repr(create_query))

    schema = OrderedDict(
        (exp.this.this, exp.kind) for exp in create_query.this.expressions
    )
    logging.debug(schema)
    return table_name, schema


def process_chunk(db, chunk, table, line_nr):
    logging.debug(f"processing chunk up to {line_nr}")

    arrow_table = pa.Table.from_pylist(chunk)
    with duckdb.connect(db) as connection:
        connection.register(f"_temp_{table}", arrow_table)
        connection.sql(f"insert into {table} from _temp_{table}")
        connection.unregister(f"_temp_{table}")
        set_processed_line(connection, line_nr)


def handle_copy(
    statement: sqlglot.Expression,
    stream,
    db,
    table,
    schema,
    chunks: int,
    index: int,
    progress: tqdm,
):
    columns = [i.this for i in statement.this.expressions]
    entries = []

    line_nr = index

    line = next(stream).rstrip()

    with duckdb.connect(db) as connection:
        last_line_nr = get_last_processed_line(connection)

    while line != r"\.":
        if line_nr > last_line_nr:
            logging.debug(f"adding {line_nr}")
            values = get_typed_values(
                OrderedDict(zip(columns, line.rstrip().split("\t"), strict=False)),
                schema,
            )
            entries.append(values)

            if len(entries) == chunks:
                process_chunk(db, chunk=entries, table=table, line_nr=line_nr)
                entries = []
        else:
            logging.debug(f"skipping {line_nr}")

        line = next(stream).rstrip()
        line_nr += 1
        progress.update(1)

    if len(entries) > 0:
        process_chunk(db, chunk=entries, table=table, line_nr=line_nr)

    return line_nr


def sql_parsing_iterator(stream, progress):
    for _block, index in get_sql_block(stream, progress):
        parsed = sqlglot.parse_one(_block)

        if parsed.key in [
            "create",
            "copy",
        ]:
            yield parsed, index


def set_processed_line(connection, line_nr):
    connection.sql(
        sqlglot.expressions.update("_stats.processed_line", {"line_nr": line_nr}).sql(
            dialect="duckdb"
        )
    )


def get_last_processed_line(connection):
    (processed_line,) = connection.sql(
        "select line_nr from _stats.processed_line limit 1"
    ).fetchone()
    return processed_line


def start(
    *args, chunks, db, output_type, files, drop_db, total, output, prefix, **kwargs
) -> None:
    if drop_db:
        pathlib.Path(db).unlink(missing_ok=True)

    TABLE_REGISTRY = {}

    with duckdb.connect(db) as connection:
        connection.sql("""
            create schema if not exists _stats;
            create table if not exists _stats.processed_line as (
                select 0 as line_nr
            )
        """)

    tqdm_params = {}
    if total:
        tqdm_params["total"] = total

    with fileinput.input(
        files=files if len(files) > 0 else ("-",), encoding="utf-8"
    ) as stream:
        progress = tqdm(unit=" lines", **tqdm_params)
        for statement, index in sql_parsing_iterator(stream, progress):
            if statement.key == "create":
                table, schema = handle_create(statement, db)
                TABLE_REGISTRY[table] = schema
            elif statement.key == "copy":
                table = statement.this.this.this.this
                schema = TABLE_REGISTRY[table]
                handle_copy(
                    statement,
                    stream,
                    db,
                    table,
                    schema,
                    chunks=chunks,
                    index=index,
                    progress=progress,
                )

    with duckdb.connect(db) as connection:
        for table in TABLE_REGISTRY.keys():
            output_path = pathlib.Path(output) / f"{prefix}{table}"
            if output_type == "parquet" or db == ":memory:":
                connection.sql(f"from {table}").write_parquet(f"{output_path}.parquet")
            else:
                raise Exception("output format not supported")


def cli():
    parser = argparse.ArgumentParser(
        prog="pg_dedump",
        description="extract tables from postgres dumps",
        add_help=True,
    )
    parser.add_argument(
        "files",
        metavar="FILE",
        nargs="*",
        help="files to read, if empty, stdin is used",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-r", "--drop-db", action="store_true")
    parser.add_argument(
        "-c",
        "--chunks",
        type=int,
        default=10000,
        help="Chunk insert size",
        required=False,
    )
    parser.add_argument(
        "-t",
        "--total",
        type=int,
        help="Total lines",
        required=False,
    )
    parser.add_argument(
        "-d",
        "--db",
        default=":memory:",
        help="Name of the dump database",
        required=False,
    )
    parser.add_argument(
        "-p",
        "--prefix",
        default="",
        help="Prefix to add to each table exported",
        required=False,
    )
    parser.add_argument(
        "-o",
        "--output",
        default="",
        help="Output path",
        required=False,
    )
    parser.add_argument(
        "--output-type",
        default="parquet",
        help="Format of the tables output",
        required=False,
    )
    args = parser.parse_args()
    logging.basicConfig(level=(logging.DEBUG if args.verbose else logging.INFO))
    start(**vars(args))


if __name__ == "__main__":
    cli()
