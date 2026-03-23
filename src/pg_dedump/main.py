import fileinput
import logging
import pathlib
from collections import OrderedDict
from typing import Annotated

import duckdb
import pyarrow as pa
import sqlglot
import sqlglot.expressions
import structlog
import typer
from tqdm import tqdm

from .helpers import get_sql_block, remove_schema

app = typer.Typer()


def configure_logger(logging_level=logging.NOTSET):
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )
    return structlog.get_logger()


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


def handle_create(statement, db, logger):
    logger.debug("handling create", statement=statement)
    create_query = remove_schema(statement)
    table_name = create_query.this.this.this.this
    with duckdb.connect(db) as connection:
        connection.sql(create_query.sql(dialect="duckdb"))

    logger.debug("using query", query=create_query)

    schema = OrderedDict(
        (exp.this.this, exp.kind) for exp in create_query.this.expressions
    )
    logger.info("create table", name=table_name)
    return table_name, schema


def process_chunk(db, chunk, table, line_nr, logger):
    logger.debug("processing chunk", line=line_nr, table=table, chunk_size=len(chunk))

    connection = duckdb.connect(db)
    try:
        arrow_table = connection.from_arrow(pa.Table.from_pylist(chunk))
        logger.debug("created arrow table", table=arrow_table)
        arrow_table.insert_into(table)
        logger.debug("inserted", length=len(chunk))
    except duckdb.BinderException as e:
        logger.error("failed to insert chunk", error=e)
    connection.close()


def handle_copy(
    statement,
    stream,
    db,
    table,
    schema,
    chunks: int,
    index: int,
    progress: tqdm,
    logger,
):
    columns = [i.this for i in statement.this.expressions]
    entries = []

    logger.info("copying", table=table)

    line_nr = index

    line = next(stream).rstrip()

    while line != r"\.":
        logger.debug(f"adding {line_nr}")
        values = get_typed_values(
            OrderedDict(zip(columns, line.rstrip().split("\t"), strict=False)),
            schema,
        )
        entries.append(values)

        if len(entries) == chunks:
            process_chunk(
                db, chunk=entries, table=table, line_nr=line_nr, logger=logger
            )
            entries = []

        line = next(stream).rstrip()
        line_nr += 1
        progress.update(1)

    if len(entries) > 0:
        process_chunk(db, chunk=entries, table=table, line_nr=line_nr, logger=logger)

    return line_nr


def sql_parsing_iterator(stream, progress, logger):
    for _block, index in get_sql_block(stream, progress):
        try:
            parsed = sqlglot.parse_one(_block)

            logger.debug("parsed SQL block", block=_block, parsed=parsed)

            if parsed.key in [
                "copy",
            ] or (parsed.key == "create" and parsed.kind == "TABLE"):
                logger.debug("handling SQL block", parsed=parsed.key, block=_block)
                yield parsed, index
        except sqlglot.ParseError as e:
            logger.warning("failed to parse SQL block", error=e)
        except sqlglot.TokenError as e:
            logger.warning("failed to tokenize SQL block", error=e)


@app.command()
def start(
    files: Annotated[
        list[str] | None,
        typer.Argument(help="Files to read, if empty, stdin is used"),
    ] = None,
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Print debug output")
    ] = False,
    drop_db: Annotated[
        bool, typer.Option("--drop-db", "-r", help="Delete the database if present")
    ] = False,
    chunks: Annotated[
        int, typer.Option("--chunks", "-c", help="Chunk insert size")
    ] = 10000,
    total: Annotated[
        int | None,
        typer.Option("--total", "-t", help="Total lines - enables the progress bar"),
    ] = None,
    db: Annotated[
        str, typer.Option("--db", "-d", help="Name of the dump database")
    ] = "dump.ddb",
    prefix: Annotated[
        str, typer.Option("--prefix", "-p", help="Prefix to add to each table exported")
    ] = "",
    output: Annotated[str, typer.Option("--output", "-o", help="Output path")] = "",
    output_type: Annotated[
        str, typer.Option("--output-type", help="Format of the tables output")
    ] = "parquet",
    custom_sql_dir: Annotated[
        str,
        typer.Option(
            "--custom-sql-dir",
            help="Path to the directory containing custom sql files for export",
        ),
    ] = "",
) -> None:
    """Extract tables from postgres dumps."""
    logger = configure_logger(logging.DEBUG if verbose else logging.INFO)
    if files is None:
        files = []
    if drop_db:
        pathlib.Path(db).unlink(missing_ok=True)

    TABLE_REGISTRY = {}

    connection = duckdb.connect(db)
    connection.install_extension("spatial")
    connection.load_extension("spatial")
    connection.install_extension("inet")
    connection.load_extension("inet")

    tqdm_params = {}
    if total:
        tqdm_params["total"] = total

    with fileinput.input(
        files=files if len(files) > 0 else ("-",), encoding="utf-8"
    ) as stream:
        progress = tqdm(unit=" lines", **tqdm_params)
        for statement, index in sql_parsing_iterator(stream, progress, logger=logger):
            if statement.key == "create":
                table, schema = handle_create(statement, db, logger=logger)
                TABLE_REGISTRY[table] = schema
            elif statement.key == "copy":
                table = statement.this.this.this.this
                if table not in TABLE_REGISTRY:
                    logger.warning(
                        "table not found for copy statement, skipping",
                        table=table,
                    )
                    continue
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
                    logger=logger,
                )

    with duckdb.connect(db) as connection:
        for table in TABLE_REGISTRY.keys():
            output_path = pathlib.Path(output) / f"{prefix}{table}"
            if output_type == "parquet":
                connection.table(table).write_parquet(
                    f"{output_path}.parquet", compression="zstd", overwrite=True
                )
            else:
                raise Exception("output format not supported")


if __name__ == "__main__":
    app()
