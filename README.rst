=============
sqliteio
=============

SQLite3 read and write library with its own API.

I have tested it with recent CPython and MicroPython.
Working with MicroPython is one of the key motivations.

Examples
-------------

Open & Close
++++++++++++++++++++++++++++++

```
import sqliteio

database = sqliteio.open('/path/to/db_name.sqlite')

(... operations to database)

database.close()
```

Fetch all records
++++++++++++++++++++++++++++++

```
for rowid_record in database.fetch_all("table_name"):
    (rowid, r) = rowid_record
    print(rowid)    # print rowid
    print(r)        # print record dict
```

Get by rowid
++++++++++++++++++++++++++++++

```
rowid_record = database.get_by_rowid("table_name", 10)
assert rowid_record[0] == 10
print(r)        # print record dict
```


Get by Primary Key
++++++++++++++++++++++++++++++

```
rowid, r = database.get_by_pk("table_name", 10)
assert rowid == 10
print(r)

```

Filter by index
++++++++++++++++++++++++++++++

```
index_schema = database.get_index_schema_by_name("index_name")
index_key = {
    "column1': 1,
    "column2": "str",
}
for rowid_record in database.filter_by_index(index_schema, index_key):
    (rowid, r) = rowid_record
    print(rowid)    # print rowid
    print(r)        # print record dict
```

Insert & Commit & Rollback
++++++++++++++++++++++++++++++

```
r1 = {
    'pk_column': None,
    'column1': 1,
    'column2': 'string1',
}
r2 = {
    'pk_column': None,
    'column1': 2,
    'column2': 'string2',
}

database.insert("table_name", [r1, r2])
```


```
database.rollback()
```

```
database.commit()
```


Reference
-------------

Reference for reading and writing the source code.

Documents on the web
++++++++++++++++++++++

- https://www.sqlite.org/fileformat2.html
- https://fly.io/blog/sqlite-internals-btree/

Repository
++++++++++++++++++++++

- https://github.com/alicebob/sqlittle

Book
++++++++++++++++++++++

- Alex Petrov, A Deep Dive into How Distributed Data Systems Work, O'Reilly Media, Inc. 2019 (chapter 3,4)

