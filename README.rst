=============
sqliteio
=============

sqliteio is a library for reading and writing SQLite3 file with its own API.

I have tested it with recent CPython and MicroPython.
Working with MicroPython is one of the key motivations.

It can be used with CPython.
But since it is written in pure python, it is too slow.
And has limited functionality.
So there is no advantage to using it with CPython.

Examples
-------------

Open & Close
++++++++++++++++++++++++++++++

Everything starts with open() and ends with close()

::

   import sqliteio
   
   database = sqliteio.open('/path/to/db_name.sqlite')
   
   (... operations to database)
   
   database.close()

open() as read only file.

::

   f = open("/path/to/db_name.sqlite", "rb")
   database = sqliteio.open(f)

You can also open() with a BytesIO instance or something byte stream generator.

::

   with open("/path/to/db_name.sqlite", "rb") as f:
       bytesio = io.BytesIO(f.read())
       database = sqliteio.open(bytesio)

Fetch all records
++++++++++++++++++++++++++++++

Retrieve all data in a table.

::

   for rowid_record in database.fetch_all("table_name"):
       (rowid, r) = rowid_record
       print(rowid)    # print rowid
       print(r)        # print record dict

Get by rowid
++++++++++++++++++++++++++++++

Retrieve a record using rowid.

Returns None if there is no record for the rowid.

::

   rowid_record = database.get_by_rowid("table_name", 10)
   assert rowid_record[0] == 10
   print(r)        # print record dict


Get by Primary Key
++++++++++++++++++++++++++++++

Retrieve a record using Primary Key.

Returns None if there is no record for the primary key.

::

   rowid, r = database.get_by_pk("table_name", 10)
   assert rowid == 10
   print(r)


Filter by index
++++++++++++++++++++++++++++++

Retrieve the target record using the index name.

::

   index_schema = database.get_index_schema_by_name("index_name")
   index_key = {
       "column1': 1,
       "column2": "str",
   }
   for rowid_record in database.filter_by_index(index_schema, index_key):
       (rowid, r) = rowid_record
       print(rowid)    # print rowid
       print(r)        # print record dict

Insert
++++++++++++++++++++++++++++++

Insert using dictionary list data.

::

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

Delete
++++++++++++++++++++++++++++++

Deletion by rowid.
In other words, sqliteio only has the ability to delete one row at a time.

::

   database.delete_by_rowid("test_table", 1)


Update
++++++++++++++++++++++++++++++

Update by rowid.
In other words, sqliteio only has the ability to update one row at a time.

::

   update_data = {
       'column1': 10,
       'column2': 'new_data',
   }
   database.update_by_rowid("test_table", 1, update_data)


Commit & Rollback
++++++++++++++++++++++++++++++

With Insert, Delete and Update, only the data in memory can be changed and reflected in the file with commit().
To discard changes, use rollback().

::

   database.commit()

::

   database.rollback()


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

