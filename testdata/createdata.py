#!/usr/bin/python3
import os
import sqlite3


def _create_test_table(f):
    try:
        os.remove(f)
    except OSError:
        pass
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("pragma page_size=512")
    cur.execute("""
        CREATE TABLE test_table(
            b varchar(255),
            c int,
            d real,
            e decimal(10, 2),
            a integer primary key not null,
            w blob,
            x date,
            y time,
            z datetime
        )""")
    cur.execute("CREATE INDEX test_idx_b_c ON test_table(b DESC, c ASC)")
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('A', '1', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"a" * 150])
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('B', '2', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"b" * 150])
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('C', '3', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"c" * 150])
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('D', '4', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"d"])

    conn.commit()
    conn.close()


def _create_no_interior_table(f):
    try:
        os.remove(f)
    except OSError:
        pass
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("pragma page_size=512")
    cur.execute("""
        CREATE TABLE test_table(
            b varchar(255),
            c int,
            d real,
            e decimal(10, 2),
            a integer primary key not null,
            w blob,
            x date,
            y time,
            z datetime
        )""")
    cur.execute("CREATE INDEX test_idx_b_c ON test_table(b, c)")
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('A', '1', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"a"])
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('B', '2', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"bb"])
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('C', '3', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"ccc"])
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('D', '4', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"dddd"])

    conn.commit()
    conn.close()


def _create_large_row_table(f):
    try:
        os.remove(f)
    except OSError:
        pass
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("pragma page_size=512")
    cur.execute("""
        CREATE TABLE test_table(
            b varchar(255),
            c int,
            d real,
            e decimal(10, 2),
            a integer primary key not null,
            w blob,
            x date,
            y time,
            z datetime
        )""")
    cur.execute("CREATE INDEX test_idx_b_c ON test_table(b, c)")
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('A', '1', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"a" * 500])
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('B', '2', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"b" * 1000])
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('C', '3', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"c" * 1500])

    conn.commit()
    conn.close()


def create_test_table():
    f = "test.sqlite"
    _create_test_table(f)


def create_test0_table():
    f = "test0.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table")
    conn.commit()
    conn.close()


def create_test1_table():
    f = "test1.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=1")
    conn.commit()
    conn.close()


def create_test2_table():
    f = "test2.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=2")
    conn.commit()
    conn.close()


def create_test3_table():
    f = "test3.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=3")
    conn.commit()
    conn.close()


def create_test4_table():
    f = "test4.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=4")
    conn.commit()
    conn.close()


def create_test13_table():
    f = "test13.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=1")
    cur.execute("DELETE FROM test_table where a=3")
    conn.commit()
    conn.close()


def create_test31_table():
    f = "test31.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=3")
    cur.execute("DELETE FROM test_table where a=1")
    conn.commit()
    conn.close()


def create_test23_table():
    f = "test23.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=2")
    cur.execute("DELETE FROM test_table where a=3")
    conn.commit()
    conn.close()


def create_test32_table():
    f = "test32.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=3")
    cur.execute("DELETE FROM test_table where a=2")
    conn.commit()
    conn.close()


def create_test24_table():
    f = "test24.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=2")
    cur.execute("DELETE FROM test_table where a=4")
    conn.commit()
    conn.close()


def create_test42_table():
    f = "test42.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=4")
    cur.execute("DELETE FROM test_table where a=2")
    conn.commit()
    conn.close()


def create_test1234_table():
    f = "test1234.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=1")
    cur.execute("DELETE FROM test_table where a=2")
    cur.execute("DELETE FROM test_table where a=3")
    cur.execute("DELETE FROM test_table where a=4")
    conn.commit()
    conn.close()


def create_test_ins5_table():
    f = "test_ins5.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('E', '5', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"e"])
    conn.commit()
    conn.close()


def create_test_ins567_table():
    f = "test_ins567.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('E', '5', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"e"])
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('F', '6', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"f"])
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('G', '7', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"g"])
    conn.commit()
    conn.close()


def create_test_ins5678_table():
    f = "test_ins5678.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('E', '5', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"e"])
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('F', '6', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"f"])
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('G', '7', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"g"])
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('H', '8', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"h"])
    conn.commit()
    conn.close()


def create_test2_ins2_table():
    f = "test2_ins2.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=2")
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('B', '2', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"b" * 150])
    conn.commit()
    conn.close()


def create_test2_ins2_short_table():
    f = "test2_ins2_short.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=2")
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('B', '2', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"b" * 149])
    conn.commit()
    conn.close()


def create_test2_ins2_long_table():
    f = "test2_ins2_long.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=2")
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('B', '2', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"b" * 151])
    conn.commit()
    conn.close()


def create_test_ins_shortshort_table():
    f = "test_ins_shortshort.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('E', '5', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"e" * 987])
    conn.commit()
    conn.close()


def create_test_ins_short_table():
    f = "test_ins_short.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('E', '5', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"e" * 988])
    conn.commit()
    conn.close()


def create_test_ins_short1_table():
    f = "test_ins_short1.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('E', '5', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"e" * 989])
    conn.commit()
    conn.close()


def create_test_ins_short2_table():
    f = "test_ins_short2.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('E', '5', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"e" * 990])
    conn.commit()
    conn.close()


def create_test_ins_middle_table():
    f = "test_ins_middle.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('E', '5', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"e" * 1000])
    conn.commit()
    conn.close()


def create_test_ins_long_table():
    f = "test_ins_long.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('E', '5', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"e" * 1154])
    conn.commit()
    conn.close()


def create_test_ins_longlong_table():
    f = "test_ins_longlong.sqlite"
    _create_test_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('E', '5', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"e" * 1155])
    conn.commit()
    conn.close()


def create_no_interior_table():
    f = "no_interior.sqlite"
    _create_no_interior_table(f)


def create_no_interior1_table():
    f = "no_interior1.sqlite"
    _create_no_interior_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=1")
    conn.commit()
    conn.close()


def create_no_interior2_table():
    f = "no_interior2.sqlite"
    _create_no_interior_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=2")
    conn.commit()
    conn.close()


def create_no_interior3_table():
    f = "no_interior3.sqlite"
    _create_no_interior_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=3")
    conn.commit()
    conn.close()


def create_no_interior4_table():
    f = "no_interior4.sqlite"
    _create_no_interior_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=4")
    conn.commit()
    conn.close()


def create_no_interior24_table():
    f = "no_interior24.sqlite"
    _create_no_interior_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=2")
    cur.execute("DELETE FROM test_table where a=4")
    conn.commit()
    conn.close()


def create_no_interior24_ins42_table():
    f = "no_interior24_ins42.sqlite"
    _create_no_interior_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=2")
    cur.execute("DELETE FROM test_table where a=4")
    cur.execute("""
        INSERT INTO test_table (a, b, c, d, e, w, x, y, z)
        VALUES (4, 'D', '4', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"dddd"])
    cur.execute("""
        INSERT INTO test_table (a, b, c, d, e, w, x, y, z)
        VALUES (2, 'B', '2', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"bb"])
    conn.commit()
    conn.close()


def create_no_interior13_table():
    f = "no_interior13.sqlite"
    _create_no_interior_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=1")
    cur.execute("DELETE FROM test_table where a=3")
    conn.commit()
    conn.close()


def create_no_interior12_table():
    f = "no_interior12.sqlite"
    _create_no_interior_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=1")
    cur.execute("DELETE FROM test_table where a=2")
    conn.commit()
    conn.close()


def create_no_interior1234_table():
    f = "no_interior1234.sqlite"
    _create_no_interior_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=1")
    cur.execute("DELETE FROM test_table where a=2")
    cur.execute("DELETE FROM test_table where a=3")
    cur.execute("DELETE FROM test_table where a=4")
    conn.commit()
    conn.close()


def create_no_interior_ins567_table():
    f = "no_interior_ins567.sqlite"
    _create_no_interior_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('E', '5', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"e"])
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('F', '6', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"f"])
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('G', '7', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"g"])
    conn.commit()
    conn.close()


def create_no_interior_ins5678_table():
    f = "no_interior_ins5678.sqlite"
    _create_no_interior_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('E', '5', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"e"])
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('F', '6', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"f"])
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('G', '7', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"g"])
    cur.execute("""
        INSERT INTO test_table (b, c, d, e, w, x, y, z)
        VALUES ('H', '8', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"h"])
    conn.commit()
    conn.close()


def create_large_row_table():
    f = "large_row.sqlite"
    _create_large_row_table(f)


def create_large_row0_table():
    f = "large_row0.sqlite"
    _create_large_row_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table")
    conn.commit()
    conn.close()


def create_large_row1_table():
    f = "large_row1.sqlite"
    _create_large_row_table(f)
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("DELETE FROM test_table where a=2")
    conn.commit()
    conn.close()


def create_without_rowid_table():
    f = "without_rowid.sqlite"
    try:
        os.remove(f)
    except OSError:
        pass
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("pragma page_size=512")
    cur.execute("""
        CREATE TABLE without_rowid_table(
            b varchar(255),
            c int,
            d real,
            e decimal(10, 2),
            a integer primary key not null,
            w blob,
            x date,
            y time,
            z datetime
        ) WITHOUT ROWID""")
    cur.execute("CREATE INDEX without_rowid_idx_b_c ON without_rowid_table(b, c)")
    cur.execute("""
        INSERT INTO without_rowid_table (a, b, c, d, e, w, x, y, z)
        VALUES (10, 'A', '1', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"bin"])
    cur.execute("""
        INSERT INTO without_rowid_table (b, a, c, d, e, w, x, y, z)
        VALUES ('B', 20, '2', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"bin"])
    cur.execute("""
        INSERT INTO without_rowid_table (b, c, a, d, e, w, x, y, z)
        VALUES ('C', '3', 30, '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"bin"])

    conn.commit()
    conn.close()


def create_str_pk_table():
    f = "str_pk.sqlite"
    try:
        os.remove(f)
    except OSError:
        pass
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("pragma page_size=512")
    cur.execute("""
        CREATE TABLE str_pk_table(
            s varchar(255) primary key,
            t varchar(255)
        )""")
    cur.execute("CREATE INDEX str_pk_idx_t ON str_pk_table(t)")
    cur.execute("INSERT INTO str_pk_table (s, t) values ('A', 'a')")
    cur.execute("INSERT INTO str_pk_table (s, t) values ('B', 'b')")
    cur.execute("INSERT INTO str_pk_table (s, t) values ('C', 'c')")

    conn.commit()
    conn.close()


def create_multi_pk_table():
    f = "multi_pk.sqlite"
    try:
        os.remove(f)
    except OSError:
        pass
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("pragma page_size=512")
    cur.execute("""
        CREATE TABLE multi_pk_table(
            a integer,
            b varchar(255),
            primary key(a, b)
        )""")
    cur.execute("CREATE INDEX multi_pk_idx_b ON multi_pk_table(b)")
    cur.execute("INSERT INTO multi_pk_table (a, b) values (1, 'A')")
    cur.execute("INSERT INTO multi_pk_table (a, b) values (2, 'A')")
    cur.execute("INSERT INTO multi_pk_table (a, b) values (3, 'A')")

    conn.commit()
    conn.close()


def create_many_record_empty_table():
    f = "many_record_empty.sqlite"
    try:
        os.remove(f)
    except OSError:
        pass
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("pragma page_size=512")
    cur.execute("""
        CREATE TABLE many_record_table(
            a integer primary key not null,
            b integer,
            c varchar(255)
        )""")
    cur.execute("CREATE INDEX many_record_idx_c ON many_record_table(c)")
    cur.execute("CREATE INDEX many_record_idx_c_desc ON many_record_table(c DESC)")

    conn.commit()
    conn.close()


def create_many_record15_table():
    f = "many_record15.sqlite"
    try:
        os.remove(f)
    except OSError:
        pass
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("pragma page_size=512")
    cur.execute("""
        CREATE TABLE many_record_table(
            a integer primary key not null,
            b integer,
            c varchar(255)
        )""")
    cur.execute("CREATE INDEX many_record_idx_c ON many_record_table(c)")
    cur.execute("CREATE INDEX many_record_idx_c_desc ON many_record_table(c DESC)")
    for i in range(1, 16):
        cur.execute("INSERT INTO many_record_table (b, c) values (?, 'aaaaaaaaaaaaaaaaaaaaaaaaaa')", [i])
    conn.commit()
    conn.close()


def create_many_record333_table():
    f = "many_record333.sqlite"
    try:
        os.remove(f)
    except OSError:
        pass
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("pragma page_size=512")
    cur.execute("""
        CREATE TABLE many_record_table(
            a integer primary key not null,
            b integer,
            c varchar(255)
        )""")
    cur.execute("CREATE INDEX many_record_idx_c ON many_record_table(c)")
    cur.execute("CREATE INDEX many_record_idx_c_desc ON many_record_table(c DESC)")
    for i in range(1, 334):
        cur.execute("INSERT INTO many_record_table (b, c) values (?, 'aaaaaaaaaaaaaaaaaaaaaaaaaa')", [i])
    conn.commit()
    conn.close()

def create_many_record334_table():
    f = "many_record334.sqlite"
    try:
        os.remove(f)
    except OSError:
        pass
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("pragma page_size=512")
    cur.execute("""
        CREATE TABLE many_record_table(
            a integer primary key not null,
            b integer,
            c varchar(255)
        )""")
    cur.execute("CREATE INDEX many_record_idx_c ON many_record_table(c)")
    cur.execute("CREATE INDEX many_record_idx_c_desc ON many_record_table(c DESC)")
    for i in range(1, 334):
        cur.execute("INSERT INTO many_record_table (b, c) values (?, 'aaaaaaaaaaaaaaaaaaaaaaaaaa')", [i])
    cur.execute("INSERT INTO many_record_table (b, c) values (?, 'abcdefghijklmnopqrstuvwxyz')", [334])

    conn.commit()
    conn.close()

def create_many_record16_table():
    f = "many_record16.sqlite"
    try:
        os.remove(f)
    except OSError:
        pass
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("pragma page_size=512")
    cur.execute("""
        CREATE TABLE many_record_table(
            a integer primary key not null,
            b integer,
            c varchar(255)
        )""")
    cur.execute("CREATE INDEX many_record_idx_c ON many_record_table(c)")
    cur.execute("CREATE INDEX many_record_idx_c_desc ON many_record_table(c DESC)")
    for i in range(1, 17):
        cur.execute("INSERT INTO many_record_table (b, c) values (?, 'aaaaaaaaaaaaaaaaaaaaaaaaaa')", [i])
    conn.commit()
    conn.close()

def create_many_record_table():
    f = "many_record.sqlite"
    try:
        os.remove(f)
    except OSError:
        pass
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("pragma page_size=512")
    cur.execute("""
        CREATE TABLE many_record_table(
            a integer primary key not null,
            b integer,
            c varchar(255)
        )""")
    cur.execute("CREATE INDEX many_record_idx_c ON many_record_table(c)")
    cur.execute("CREATE INDEX many_record_idx_c_desc ON many_record_table(c DESC)")
    for i in range(1, 334):
        cur.execute("INSERT INTO many_record_table (b, c) values (?, 'aaaaaaaaaaaaaaaaaaaaaaaaaa')", [i])
    for i in range(334, 667):
        cur.execute("INSERT INTO many_record_table (b, c) values (?, 'abcdefghijklmnopqrstuvwxyz')", [i])
    for i in range(667, 1000):
        cur.execute("INSERT INTO many_record_table (b, c) values (?, 'zzzzzzzzzzzzzzzzzzzzzzzzzz')", [i])

    conn.commit()
    conn.close()


def create_without_rowid_many_record_table():
    f = "without_rowid_many_record.sqlite"
    try:
        os.remove(f)
    except OSError:
        pass
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("pragma page_size=512")
    cur.execute("""
        CREATE TABLE without_rowid_many_record_table(
            a integer primary key not null,
            b varchar(255)
        ) WITHOUT ROWID""")
    cur.execute("CREATE INDEX without_rowid_many_record_idx_a_b ON without_rowid_many_record_table(a, b)")
    for i in range(1, 1000):
        cur.execute("INSERT INTO without_rowid_many_record_table (a, b) values (?, 'abcdefghijklmnopqrstuvwxyz')", [i])

    conn.commit()
    conn.close()


def create_str_pk_many_record_table():
    f = "str_pk_many_record.sqlite"
    try:
        os.remove(f)
    except OSError:
        pass
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("pragma page_size=512")
    cur.execute("""
        CREATE TABLE str_pk_many_record_table(
            s varchar(255) primary key,
            t varchar(255)
        )""")
    cur.execute("CREATE INDEX str_pk_idx_t ON str_pk_many_record_table(t)")
    for i in range(1, 500):
        cur.execute("INSERT INTO str_pk_many_record_table (s, t) values (?, ?)", [str(i), str(i)])

    conn.commit()
    conn.close()


def create_multi_pk_many_record_table():
    f = "multi_pk_many_record.sqlite"
    try:
        os.remove(f)
    except OSError:
        pass
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("pragma page_size=512")
    cur.execute("""
        CREATE TABLE multi_pk_many_record_table(
            a integer,
            b varchar(255),
            primary key(a, b)
        )""")

    for i in range(1, 1000):
        cur.execute("INSERT INTO multi_pk_many_record_table (a, b) values (?, 'aaaaaaaaaaaaaaaaaaaa')", [i])

    conn.commit()
    conn.close()


def create_pk_fk_table():
    f = "pk_fk.sqlite"
    try:
        os.remove(f)
    except OSError:
        pass
    conn = sqlite3.connect(f)
    cur = conn.cursor()
    cur.execute("pragma page_size=512")
    cur.execute("""
        CREATE TABLE base_table(
            a integer primary key not null,
            b varchar(255),
            c int,
            d real,
            e decimal(10, 2),
            w blob,
            x date,
            y time,
            z datetime
        )""")
    cur.execute("""
        CREATE TABLE fk_table(
            id integer primary key,
            fk int,
            s varchar(255),
            foreign key (fk) references base_table(a)
        )""")
    cur.execute("PRAGMA foreign_keys=True")
    cur.execute("""
        INSERT INTO base_table (a, b, c, d, e, w, x, y, z)
        VALUES (10, 'A', '1', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"bin"])
    cur.execute("""
        INSERT INTO base_table (a, b, c, d, e, w, x, y, z)
        VALUES (20, 'B', '2', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"bin"])
    cur.execute("""
        INSERT INTO base_table (a, b, c, d, e, w, x, y, z)
        VALUES (30, 'C', '3', '1.23', '1.23', ?,
                '1967-08-11', '12:34:45', '1967-08-11 12:34:45')""", [b"bin"])

    cur.execute("INSERT INTO fk_table (fk, s) values (10, 'abc')")
    cur.execute("INSERT INTO fk_table (fk, s) values (20, 'def')")
    cur.execute("INSERT INTO fk_table (fk, s) values (30, 'ghi')")

    conn.commit()
    conn.close()


if __name__ == "__main__":
    create_test_table()
    create_test0_table()
    create_test1_table()
    create_test2_table()
    create_test3_table()
    create_test4_table()
    create_test13_table()
    create_test31_table()
    create_test23_table()
    create_test32_table()
    create_test24_table()
    create_test42_table()
    create_test1234_table()
    create_test_ins5_table()
    create_test_ins567_table()
    create_test_ins5678_table()
    create_test2_ins2_table()
    create_test2_ins2_short_table()
    create_test2_ins2_long_table()
    create_test_ins_shortshort_table()
    create_test_ins_short_table()
    create_test_ins_short1_table()
    create_test_ins_short2_table()
    create_test_ins_middle_table()
    create_test_ins_long_table()
    create_test_ins_longlong_table()

    create_no_interior_table()
    create_no_interior1_table()
    create_no_interior2_table()
    create_no_interior3_table()
    create_no_interior4_table()
    create_no_interior24_table()
    create_no_interior24_ins42_table()
    create_no_interior13_table()
    create_no_interior12_table()
    create_no_interior1234_table()
    create_no_interior_ins567_table()
    create_no_interior_ins5678_table()

    create_large_row_table()
    create_large_row0_table()
    create_large_row1_table()

    create_without_rowid_table()
    create_str_pk_table()
    create_multi_pk_table()
    create_many_record_empty_table()
    create_many_record15_table()
    create_many_record16_table()
    create_many_record333_table()
    create_many_record334_table()
    create_many_record_table()
    create_without_rowid_many_record_table()
    create_str_pk_many_record_table()
    create_multi_pk_many_record_table()

    create_pk_fk_table()
