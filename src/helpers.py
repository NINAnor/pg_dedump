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


def safe_parse(text, dialect="postgres"):
    try:
        return sqlglot.parse_one(text, dialect=dialect)
    except sqlglot.errors.ParseError as e:
        print("errors", e.errors)

    return None


def remove_schema(expression):
    def transformer(node):
        # print(repr(node))
        if node.key == "table":
            del node.args["db"]
        if (
            isinstance(node, sqlglot.expressions.DataType)
            and node.this == sqlglot.expressions.DataType.Type.USERDEFINED
        ):
            return sqlglot.expressions.DataType(
                this=sqlglot.expressions.DataType.Type.VARCHAR
            )
        if (
            isinstance(node, sqlglot.expressions.DataType)
            and node.this == sqlglot.expressions.DataType.Type.GEOMETRY
        ):
            return sqlglot.expressions.DataType(
                this=sqlglot.expressions.DataType.Type.GEOMETRY
            )
        return node

    return expression.transform(transformer)
