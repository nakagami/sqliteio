import re


# https://www.sqlite.org/lang_createtable.html
# https://www.sqlite.org/datatype3.html

TOK_NAME = 1
TOK_AUTOINCREMENT = 2
TOK_PRIMARY_KEY = 3
TOK_UNIQUE_KEY = 4
TOK_CHECK = 5
TOK_FOREIGN_KEY = 6
TOK_NULL = 7
TOK_NOT_NULL = 9
TOK_DEFAULT = 10

TYPE_NULL = 20
# INTGER
TYPE_INTEGER = 21
# TEXT
TYPE_TEXT = 22
# BLOB
TYPE_BLOB = 23
# REAL
TYPE_REAL = 24
TYPE_FLOAT = 25
# NUMERIC
TYPE_NUMERIC = 26
TYPE_DECIMAL = 27
TYPE_BOOL = 28
TYPE_DATE = 29
TYPE_TIME = 30
TYPE_DATETIME = 31

__all__ = ("TableSchema", "TableColumn", "IndexSchema", "ViewSchema")

reserved_keywords = [
    "UNSIGNED", "BIG", "INT",
    "INTEGER", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT", "INT2", "INT8",
    "CHARACTER", "VARCHAR", "VARYING", "NCHAR", "NATIVE", "NVARCHAR", "CLOB", "TEXT",
    "BLOB", "REAL", "DOUBLE", "PRECISION", "FLOAT",
    "NUMERIC", "DECIMAL", "BOOLEAN", "DATE", "TIME", "DATETIME",
    "PRIMARY", "FOREIGN", "UNIQUE", "KEY", "AUTOINCREMENT", "NOT", "NULL", "DEFAULT",
    "INDEX", "ON", "ASC", "DESC", "WITHOUT", "ROWID",
]


def _is_match_tokens(tokens, start, keywords):
    if len(tokens) - start < len(keywords):
        return False
    for i in range(len(keywords)):
        if keywords[i] is None:
            continue
        if keywords[i] != tokens[start + i]:
            return False
    return True


def _split_tokens(s):
    "string split to SQL tokens"
    results = []
    while s:
        while s and s[0].isspace():
            s = s[1:]
        if s and s[0] in (',', '(', ')'):
            results.append(s[:1])
            s = s[1:]
        elif s:
            v = ''
            while s and not (s[0].isspace() or s[0] in (',', '(', ')')):
                v += s[0]
                s = s[1:]
            results.append(v)
    return results


def _parse_parentheses(tokens, start):
    "expression with parentheses to list"
    values = []
    assert tokens[start] == '('
    i = start + 1
    while tokens[i] != ')':
        value = []
        while i < len(tokens) and tokens[i] not in (')', ','):
            value.append(tokens[i])
            i += 1
        values.append(value)
        if tokens[i] == ',':
            i += 1
    return values, i + 1


class TableColumn:
    def __init__(self, pos, name, tokens, start):
        self.pos = pos
        self.name = name
        self.tokens = tokens
        self.column_type = TYPE_TEXT
        self.max_length = -1
        self.precision = -1
        self.scale = -1
        start = self._parse_type(start)
        self.is_primary_key = False
        self.is_autoincrement = False
        self.is_unique = False
        self.has_check = False
        self.nullable = False
        while start < len(tokens):
            start = self._parse_column_definition(start)
        self.is_rowid = False

    def _parse_type(self, start):
        # https://www.sqlite.org/datatype3.html
        # 3.1. Determination Of Column Affinity
        if _is_match_tokens(self.tokens, start, ["DECIMAL", "(", None, ",", None, ")"]):
            self.column_type = TYPE_DECIMAL
            self.precision = int(self.tokens[start+2])
            self.scale = int(self.tokens[start+4])
            return start + 6

        tokens_type = [
            (["CHARACTER", "(", None, ")"], TYPE_TEXT),
            (["VARCHAR", "(", None, ")"], TYPE_TEXT),
            (["VARYING", "CHARACTER", "(", None, ")"], TYPE_TEXT),
            (["NCHAR", "(", None, ")"], TYPE_TEXT),
            (["NATIVE", "CHARACTER", "(", None, ")"], TYPE_TEXT),
            (["NVARCHAR", "(", None, ")"], TYPE_TEXT),
        ]
        for tokens, column_type in tokens_type:
            if _is_match_tokens(self.tokens, start, tokens):
                self.column_type = column_type
                self.max_length = int(self.tokens[start + len(tokens) - 2])
                return start + len(tokens)

        tokens_type = [
            (["INT"], TYPE_INTEGER),
            (["INTEGER"], TYPE_INTEGER),
            (["TINYINT"], TYPE_INTEGER),
            (["SMALLINT"], TYPE_INTEGER),
            (["MEDIUMINT"], TYPE_INTEGER),
            (["BIGINT"], TYPE_INTEGER),
            (["UNSIGNED", "BIG", "INT"], TYPE_INTEGER),
            (["INT2"], TYPE_INTEGER),
            (["INT8"], TYPE_INTEGER),
            (["TEXT"], TYPE_TEXT),
            (["CLOB"], TYPE_TEXT),
            (["VARCHAR"], TYPE_TEXT),
            (["BLOB"], TYPE_BLOB),
            (["REAL"], TYPE_REAL),
            (["DOUBLE"], TYPE_FLOAT),
            (["DOUBLE", "PRECISION"], TYPE_FLOAT),
            (["FLOAT"], TYPE_FLOAT),
            (["NUMERIC"], TYPE_NUMERIC),
            (["BOOLEAN"], TYPE_BOOL),
            (["DATE"], TYPE_DATE),
            (["TIME"], TYPE_TIME),
            (["DATETIME"], TYPE_DATETIME),
        ]
        for tokens, column_type in tokens_type:
            if _is_match_tokens(self.tokens, start, tokens):
                self.column_type = column_type
                return start + len(tokens)

        raise ValueError("Unknow type {}".format(self.tokens))

    def _parse_column_definition(self, start):
        # https://www.sqlite.org/syntax/column-constraint.html
        if _is_match_tokens(self.tokens, start, ["PRIMARY", "KEY"]):
            self.is_primary_key = True
            return start + 2
        elif _is_match_tokens(self.tokens, start, ["NOT", "NULL"]):
            self.nullable = False
            return start + 2
        elif _is_match_tokens(self.tokens, start, ["NULL"]):
            self.nullable = False
            return start + 1
        elif _is_match_tokens(self.tokens, start, ["UNIQUE"]):
            self.is_unique = True
            return start + 1
        elif _is_match_tokens(self.tokens, start, ["AUTOINCREMENT"]):
            self.is_autoincrement = True
            return start + 1
        return start + 1

    def __repr__(self):
        return "{}:{}".format(self.name, "/".join(self.tokens))


class BaseSchema:
    def __init__(self, name, table_name, pgno, sql):
        self.name = name
        self.table_name = table_name
        self.pgno = pgno
        self.sql = sql

    def __repr__(self):
        return "{}:{}:{}:{}".format(self.name, self.table_name, self.pgno, self.sql)


class TableSchema(BaseSchema):
    def __init__(self, name, table_name, pgno, sql, database):
        super().__init__(name, table_name, pgno, sql)
        self.database = database
        self.columns = []
        primary_keys = []

        definitions = self._split_definitions()
        for d in definitions:
            tokens = _split_tokens(d)
            for i in range(len(tokens)):
                s = tokens[i].upper()
                if s in reserved_keywords:
                    tokens[i] = s
            tok, value, next_i = self._parse_column_name_or_table_constraint(tokens)
            if tok == TOK_NAME:
                pos = len(self.columns)
                self.columns.append(TableColumn(pos, value, tokens, next_i))
            if tok == TOK_PRIMARY_KEY:
                primary_keys = value

        create_table_option = self.sql[self.sql.rfind(')'):].upper()
        self.without_rowid = bool(re.search(r'WITHOUT\s+ROWID', create_table_option))

        # find primary key
        if not primary_keys:
            for c in self.columns:
                if c.is_primary_key:
                    primary_keys.append(c.name)
        self.primary_keys = [self.get_column_by_name(s) for s in primary_keys]

        # find rowid
        if not self.without_rowid:
            for c in self.columns:
                if c.column_type == TYPE_INTEGER and c.is_primary_key and not c.nullable:
                    c.is_rowid = True
                    break

        if self.without_rowid:
            # primary key to left
            for c in self.columns:
                if c.is_primary_key:
                    c.pos = -1
            self.columns = sorted(self.columns, key=lambda c: c.pos)
            for i, c in enumerate(self.columns):
                c.pos = i

    def _split_definitions(self):
        start = self.sql.find('(') + 1
        end = self.sql.rfind(')')
        par_level = 0

        definitions = []
        s = ""
        for i in range(start, end):
            c = self.sql[i]
            if c == '(':
                par_level += 1
            elif c == ')':
                par_level -= 1
            if c == ',' and par_level == 0:
                definitions.append(s)
                s = ""
                continue
            s += c
        if s:
            definitions.append(s)

        return definitions

    def _parse_table_constraint(self, tokens, start):
        # https://www.sqlite.org/syntax/table-constraint.html
        # PRIMARY KEY ( column1, column2... )
        # UNIQUE ( column1, column2... )
        # CHECK ( expr )
        # FOREIGN KEY ( column1, column2... )
        if _is_match_tokens(tokens, start, ["PRIMARY", "KEY", "("]):
            values, start = _parse_parentheses(tokens, start + 2)
            values = [v[0] for v in values]     # flatten
            return TOK_PRIMARY_KEY, values, start
        elif _is_match_tokens(tokens, start, ["UNIQUE", "("]):
            values, start = _parse_parentheses(tokens, start + 1)
            return TOK_UNIQUE_KEY, values, start
        elif _is_match_tokens(tokens, start, ["CHECK", "("]):
            values, start = _parse_parentheses(tokens, start + 1)
            return TOK_CHECK, values, start
        elif _is_match_tokens(tokens, start, ["FOREIGN", "KEY", "("]):
            values, start = _parse_parentheses(tokens, start + 2)
            return TOK_FOREIGN_KEY, values, start
        # Other
        return None, [], start

    def _parse_column_name_or_table_constraint(self, tokens):
        tok_table_constraint, value, _ = self._parse_table_constraint(tokens, 0)
        if tok_table_constraint:
            return (tok_table_constraint, value, len(tokens))
        column_name = tokens[0]
        # unquote column name
        if column_name[0] == '"' and column_name[-1] == '"':
            column_name = column_name[1:-1]
        if column_name[0] == '`' and column_name[-1] == '`':
            column_name = column_name[1:-1]
        return (TOK_NAME, column_name, 1)

    def row_converter(self, rowid, record):
        return (rowid, {
            c.name: rowid if c.is_rowid else r
            for r, c in zip(record, self.columns)
        })

    @property
    def column_names(self):
        return [c.name for c in self.columns]

    def get_column_by_name(self, name):
        for column in self.columns:
            if column.name == name:
                return column
        return None

    def dict_to_value_list(self, d):
        value_list = []
        rowid = None

        for column in self.columns:
            v = d.get(column.name)
            if column.is_rowid:
                rowid = v
                v = None
            value_list.append(v)

        return rowid, value_list


class IndexSchema(BaseSchema):
    def __init__(self, name, table_name, pgno, sql, table_schema):
        super().__init__(name, table_name, pgno, sql)
        if sql:
            self.is_primary_key = False
            self.tokens = _split_tokens(sql)
            if _is_match_tokens(self.tokens, 0, ["CREATE", "INDEX", None, "ON", None, "("]):
                values, start = _parse_parentheses(self.tokens, 5)
                column_names = [v[0] for v in values]     # flatten
                self.columns = [table_schema.get_column_by_name(name) for name in column_names]
                # ASC:1 DESC:-1
                self.orders = [-1 if len(v) > 1 and v[1] == "DESC" else 1 for v in values]
            else:
                raise NotImplementedError("Can't parse:{}".format(self.tokens))
        else:
            self.is_primary_key = True
            self.columns = table_schema.primary_keys
            self.orders = [1] * len(self.columns)


class ViewSchema(BaseSchema):
    def __init__(self, name, table_name, pgno, sql):
        super().__init__(name, table_name, pgno, sql)
