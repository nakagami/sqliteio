################################################################################
# MIT License
#
# Copyright (c) 2023 Hajime Nakagami<nakagami@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
################################################################################
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
    "CONSTRAINT", "CHECK", "REFERENCES", "COLLATE", "GENERATED", "ALWAYS", "AS",
    "VIRTUAL", "STORED", "STRICT",
]

column_constraint_keywords = {
    "CONSTRAINT",
    "PRIMARY",
    "NOT",
    "NULL",
    "UNIQUE",
    "CHECK",
    "DEFAULT",
    "COLLATE",
    "REFERENCES",
    "GENERATED",
    "AUTOINCREMENT",
}


def _is_match_tokens(tokens, start, keywords):
    if len(tokens) - start < len(keywords):
        return False
    for i in range(len(keywords)):
        if keywords[i] is None:
            continue
        if keywords[i] != tokens[start + i]:
            return False
    return True


def _unquote_identifier(s):
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        return s[1:-1].replace('""', '"')
    if len(s) >= 2 and s[0] == "`" and s[-1] == "`":
        return s[1:-1].replace("``", "`")
    if len(s) >= 2 and s[0] == "[" and s[-1] == "]":
        return s[1:-1].replace("]]", "]")
    return s


def _split_tokens(s):
    "string split to SQL tokens"
    results = []
    i = 0
    while i < len(s):
        while i < len(s) and s[i].isspace():
            i += 1
        if i >= len(s):
            break
        if s[i] in (",", "(", ")"):
            results.append(s[i])
            i += 1
            continue
        if s[i] in ("'", '"', "`"):
            quote = s[i]
            j = i + 1
            while j < len(s):
                if s[j] == quote:
                    if j + 1 < len(s) and s[j + 1] == quote:
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            results.append(s[i:j])
            i = j
            continue
        if s[i] == "[":
            j = i + 1
            while j < len(s):
                if s[j] == "]":
                    if j + 1 < len(s) and s[j + 1] == "]":
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            results.append(s[i:j])
            i = j
            continue

        j = i
        while j < len(s) and not (s[j].isspace() or s[j] in (",", "(", ")")):
            j += 1
        results.append(s[i:j])
        i = j
    return results


def _parse_parentheses(tokens, start):
    "expression with parentheses to list"
    values = []
    assert tokens[start] == '('
    i = start + 1
    level = 0
    value = []
    while i < len(tokens):
        token = tokens[i]
        if token == '(':
            level += 1
            value.append(token)
        elif token == ')':
            if level == 0:
                values.append(value)
                return values, i + 1
            level -= 1
            value.append(token)
        elif token == ',' and level == 0:
            values.append(value)
            value = []
        else:
            value.append(token)
        i += 1
    raise ValueError("Unclosed parentheses:{}".format(tokens[start:]))


def _is_column_constraint_start(tokens, start):
    if start >= len(tokens):
        return False
    return tokens[start] in column_constraint_keywords


class TableColumn:
    def __init__(self, pos, name, tokens, start):
        self.pos = pos
        self.name = name
        self.tokens = tokens
        self.column_type = TYPE_TEXT
        self.declared_type = ""
        self.max_length = -1
        self.precision = -1
        self.scale = -1
        start = self._parse_type(start)
        self.is_primary_key = False
        self.primary_key_order = 1
        self.is_autoincrement = False
        self.is_unique = False
        self.has_check = False
        self.nullable = True
        while start < len(tokens):
            start = self._parse_column_definition(start)
        self.is_rowid = False

    def _parse_type(self, start):
        # https://www.sqlite.org/datatype3.html
        # 3.1. Determination Of Column Affinity
        if start >= len(self.tokens):
            return start
        if _is_column_constraint_start(self.tokens, start):
            return start

        i = start
        par_level = 0
        type_tokens = []
        while i < len(self.tokens):
            if par_level == 0 and _is_column_constraint_start(self.tokens, i):
                break
            token = self.tokens[i]
            if token == '(':
                par_level += 1
            elif token == ')' and par_level > 0:
                par_level -= 1
            type_tokens.append(token)
            i += 1

        if not type_tokens:
            return start

        leading_tokens = []
        for token in type_tokens:
            if token == '(':
                break
            if token not in (",", ")"):
                leading_tokens.append(token.upper())
        self.declared_type = " ".join(leading_tokens)

        if _is_match_tokens(type_tokens, 0, ["DECIMAL", "(", None, ",", None, ")"]):
            self.column_type = TYPE_DECIMAL
            self.precision = int(type_tokens[2])
            self.scale = int(type_tokens[4])
            return i

        if _is_match_tokens(type_tokens, 0, ["CHARACTER", "(", None, ")"]):
            self.column_type = TYPE_TEXT
            self.max_length = int(type_tokens[2])
            return i
        if _is_match_tokens(type_tokens, 0, ["VARCHAR", "(", None, ")"]):
            self.column_type = TYPE_TEXT
            self.max_length = int(type_tokens[2])
            return i
        if _is_match_tokens(type_tokens, 0, ["VARYING", "CHARACTER", "(", None, ")"]):
            self.column_type = TYPE_TEXT
            self.max_length = int(type_tokens[3])
            return i
        if _is_match_tokens(type_tokens, 0, ["NCHAR", "(", None, ")"]):
            self.column_type = TYPE_TEXT
            self.max_length = int(type_tokens[2])
            return i
        if _is_match_tokens(type_tokens, 0, ["NATIVE", "CHARACTER", "(", None, ")"]):
            self.column_type = TYPE_TEXT
            self.max_length = int(type_tokens[3])
            return i
        if _is_match_tokens(type_tokens, 0, ["NVARCHAR", "(", None, ")"]):
            self.column_type = TYPE_TEXT
            self.max_length = int(type_tokens[2])
            return i

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
            if _is_match_tokens(type_tokens, 0, tokens):
                self.column_type = column_type
                return i

        # SQLite affinity rules for arbitrary type names.
        affinity_key = "".join(t.upper() for t in type_tokens if t not in (",", "(", ")"))
        if "INT" in affinity_key:
            self.column_type = TYPE_INTEGER
        elif "CHAR" in affinity_key or "CLOB" in affinity_key or "TEXT" in affinity_key:
            self.column_type = TYPE_TEXT
        elif "BLOB" in affinity_key:
            self.column_type = TYPE_BLOB
        elif "REAL" in affinity_key or "FLOA" in affinity_key or "DOUB" in affinity_key:
            self.column_type = TYPE_REAL
        elif "DATETIME" in affinity_key:
            self.column_type = TYPE_DATETIME
        elif "DATE" in affinity_key:
            self.column_type = TYPE_DATE
        elif "TIME" in affinity_key:
            self.column_type = TYPE_TIME
        elif "BOOL" in affinity_key:
            self.column_type = TYPE_BOOL
        else:
            self.column_type = TYPE_NUMERIC
        return i

    def _parse_column_definition(self, start):
        # https://www.sqlite.org/syntax/column-constraint.html
        if _is_match_tokens(self.tokens, start, ["CONSTRAINT", None]):
            return start + 2
        if _is_match_tokens(self.tokens, start, ["PRIMARY", "KEY"]):
            self.is_primary_key = True
            i = start + 2
            if i < len(self.tokens) and self.tokens[i] in ("ASC", "DESC"):
                self.primary_key_order = -1 if self.tokens[i] == "DESC" else 1
                i += 1
            return i
        if _is_match_tokens(self.tokens, start, ["NOT", "NULL"]):
            self.nullable = False
            return start + 2
        if _is_match_tokens(self.tokens, start, ["NULL"]):
            self.nullable = True
            return start + 1
        if _is_match_tokens(self.tokens, start, ["UNIQUE"]):
            self.is_unique = True
            return start + 1
        if _is_match_tokens(self.tokens, start, ["AUTOINCREMENT"]):
            self.is_autoincrement = True
            return start + 1
        if _is_match_tokens(self.tokens, start, ["CHECK", "("]):
            self.has_check = True
            _, i = _parse_parentheses(self.tokens, start + 1)
            return i
        if _is_match_tokens(self.tokens, start, ["DEFAULT"]):
            i = start + 1
            if i < len(self.tokens) and self.tokens[i] == '(':
                _, i = _parse_parentheses(self.tokens, i)
                return i
            return min(i + 1, len(self.tokens))
        if _is_match_tokens(self.tokens, start, ["COLLATE", None]):
            return start + 2
        if _is_match_tokens(self.tokens, start, ["REFERENCES", None]):
            i = start + 2
            if i < len(self.tokens) and self.tokens[i] == '(':
                _, i = _parse_parentheses(self.tokens, i)
            return i
        if _is_match_tokens(self.tokens, start, ["GENERATED"]):
            i = start + 1
            if _is_match_tokens(self.tokens, i, ["ALWAYS"]):
                i += 1
            if _is_match_tokens(self.tokens, i, ["AS", "("]):
                _, i = _parse_parentheses(self.tokens, i + 1)
            if i < len(self.tokens) and self.tokens[i] in ("VIRTUAL", "STORED"):
                i += 1
            return i
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
        self.columns = []                   # list[TableColumn]
        self.primary_keys = []              # list[str]
        self.foreign_key_constraints = []   # list[(list[str], str, list[str])]
        self.check_constraints = []         # list[str]
        self.unique_key_constraints = []    # list[list[str]]
        self._table_primary_key_orders = []

        definitions = self._split_definitions()
        for d in definitions:
            tokens = _split_tokens(d)
            if not tokens:
                continue
            for i in range(len(tokens)):
                s = tokens[i].upper()
                if s in reserved_keywords:
                    tokens[i] = s
            tok, value, next_i = self._parse_column_name_or_table_constraint(tokens)
            if tok == TOK_NAME:
                pos = len(self.columns)
                self.columns.append(TableColumn(pos, value, tokens, next_i))
            elif tok == TOK_PRIMARY_KEY:
                self.primary_keys = [v[0] for v in value]
                self._table_primary_key_orders = [v[1] for v in value]
            elif tok == TOK_UNIQUE_KEY:
                self.unique_key_constraints.append([_unquote_identifier(v[0]) for v in value if v])
            elif tok == TOK_CHECK:
                self.check_constraints.append(" ".join(value))
            elif tok == TOK_FOREIGN_KEY:
                self.foreign_key_constraints.append(value)

        create_table_option = self.sql[self.sql.rfind(')'):].upper()
        self.without_rowid = bool(re.search(r'WITHOUT\s+ROWID', create_table_option))
        self.strict = bool(re.search(r'\bSTRICT\b', create_table_option))

        # find primary key
        if not self.primary_keys:
            for c in self.columns:
                if c.is_primary_key:
                    self.primary_keys.append(c.name)
                    self._table_primary_key_orders.append(c.primary_key_order)

        # find rowid alias
        if not self.without_rowid and len(self.primary_keys) == 1:
            c = self.get_column_by_name(self.primary_keys[0])
            if c:
                order = self._table_primary_key_orders[0] if self._table_primary_key_orders else c.primary_key_order
                if c.declared_type == "INTEGER" and order != -1:
                    c.is_rowid = True

        if self.without_rowid:
            # primary key to left
            primary_keys = set(self.primary_keys)
            for c in self.columns:
                if c.name in primary_keys:
                    c.pos = -1
            self.columns = sorted(self.columns, key=lambda c: c.pos)
            for i, c in enumerate(self.columns):
                c.pos = i

    def _dump(self):
        print(self.sql)

    def _split_definitions(self):
        start = self.sql.find('(') + 1
        end = self.sql.rfind(')')
        par_level = 0

        definitions = []
        s = ""
        quote = None
        bracket_mode = False
        i = start
        while i < end:
            c = self.sql[i]
            if quote:
                s += c
                if c == quote:
                    if i + 1 < end and self.sql[i + 1] == quote:
                        s += self.sql[i + 1]
                        i += 1
                    else:
                        quote = None
                i += 1
                continue
            if bracket_mode:
                s += c
                if c == ']':
                    if i + 1 < end and self.sql[i + 1] == ']':
                        s += self.sql[i + 1]
                        i += 1
                    else:
                        bracket_mode = False
                i += 1
                continue

            if c in ("'", '"', "`"):
                quote = c
                s += c
                i += 1
                continue
            if c == '[':
                bracket_mode = True
                s += c
                i += 1
                continue

            if c == '(':
                par_level += 1
            elif c == ')':
                par_level -= 1
            if c == ',' and par_level == 0:
                definitions.append(s)
                s = ""
                i += 1
                continue
            s += c
            i += 1
        if s:
            definitions.append(s)

        return definitions

    def _parse_table_constraint(self, tokens, start):
        # https://www.sqlite.org/syntax/table-constraint.html
        # [CONSTRAINT name]
        # PRIMARY KEY ( indexed-column )
        # UNIQUE ( indexed-column )
        # CHECK ( expr )
        # FOREIGN KEY ( column1, column2... ) REFERENCES table(...)
        if _is_match_tokens(tokens, start, ["CONSTRAINT", None]):
            start += 2

        if _is_match_tokens(tokens, start, ["PRIMARY", "KEY", "("]):
            values, start = _parse_parentheses(tokens, start + 2)
            indexed_columns = []
            for v in values:
                if not v:
                    continue
                order = 1
                for token in v[1:]:
                    if token == "DESC":
                        order = -1
                        break
                    if token == "ASC":
                        order = 1
                        break
                indexed_columns.append((_unquote_identifier(v[0]), order))
            return TOK_PRIMARY_KEY, indexed_columns, start
        if _is_match_tokens(tokens, start, ["UNIQUE", "("]):
            values, start = _parse_parentheses(tokens, start + 1)
            return TOK_UNIQUE_KEY, values, start
        if _is_match_tokens(tokens, start, ["CHECK", "("]):
            values, start = _parse_parentheses(tokens, start + 1)
            expr_tokens = []
            for i, v in enumerate(values):
                if i:
                    expr_tokens.append(",")
                expr_tokens.extend(v)
            return TOK_CHECK, expr_tokens, start
        if _is_match_tokens(tokens, start, ["FOREIGN", "KEY", "("]):
            values, start = _parse_parentheses(tokens, start + 2)
            local_columns = [_unquote_identifier(v[0]) for v in values if v]
            ref_table = ""
            ref_columns = []
            if _is_match_tokens(tokens, start, ["REFERENCES", None]):
                ref_table = _unquote_identifier(tokens[start + 1])
                start += 2
                if start < len(tokens) and tokens[start] == "(":
                    ref_values, start = _parse_parentheses(tokens, start)
                    ref_columns = [_unquote_identifier(v[0]) for v in ref_values if v]
            return TOK_FOREIGN_KEY, (local_columns, ref_table, ref_columns), start
        # Other
        return None, [], start

    def _parse_column_name_or_table_constraint(self, tokens):
        tok_table_constraint, value, _ = self._parse_table_constraint(tokens, 0)
        if tok_table_constraint:
            return (tok_table_constraint, value, len(tokens))
        column_name = _unquote_identifier(tokens[0])
        return (TOK_NAME, column_name, 1)

    def row_converter(self, rowid, record):
        return (rowid, {
            c.name: rowid if c.is_rowid else r
            for r, c in zip(record, self.columns)
        })

    @property
    def column_names(self):
        return [c.name for c in self.columns]

    @property
    def primary_key_columns(self):
        return [self.get_column_by_name(s) for s in self.primary_keys]

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
                start = 5
            elif _is_match_tokens(self.tokens, 0, ["CREATE", "UNIQUE", "INDEX", None, "ON", None, "("]):
                start = 6
            else:
                raise NotImplementedError("Can't parse:{}".format(self.tokens))
            values, _ = _parse_parentheses(self.tokens, start)
            column_names = [_unquote_identifier(v[0]) for v in values if v]
            self.columns = [table_schema.get_column_by_name(name) for name in column_names]
            # ASC:1 DESC:-1
            self.orders = [-1 if len(v) > 1 and "DESC" in v[1:] else 1 for v in values]
        else:
            self.is_primary_key = True
            self.columns = [table_schema.get_column_by_name(name) for name in table_schema.primary_keys]
            self.orders = [1] * len(self.columns)

    def _dump(self):
        print(self.sql)


class ViewSchema(BaseSchema):
    def __init__(self, name, table_name, pgno, sql):
        super().__init__(name, table_name, pgno, sql)

    def _dump(self):
        print(self.sql)
