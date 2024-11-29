import sqlglot


def get_sql_block(input):  # noqa: A002
    text_block = ""
    for line in input:
        if line.lstrip().startswith("--"):
            continue
        text_block += line.rstrip()
        if text_block.endswith(";"):
            yield text_block
            text_block = ""


def remove_schema(expression):
    def transformer(node):
        if node.key == "table":
            del node.args["db"]
        if (
            isinstance(node, sqlglot.expressions.DataType)
            and node.this == sqlglot.expressions.DataType.Type.USERDEFINED
        ):
            # convert Postgres custom types to strings
            return sqlglot.expressions.DataType(
                this=sqlglot.expressions.DataType.Type.VARCHAR
            )
        if (
            isinstance(node, sqlglot.expressions.DataType)
            and node.this == sqlglot.expressions.DataType.Type.GEOMETRY
        ):
            # Postgres dumps use hexewkb strings
            return sqlglot.expressions.DataType(
                this=sqlglot.expressions.DataType.Type.VARCHAR
            )
        return node

    return expression.transform(transformer)
