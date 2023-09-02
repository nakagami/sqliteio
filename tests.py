#!/usr/bin/env python3
import io
import unittest
import binascii

import sqliteio
from sqliteio import record


class TestRecord(unittest.TestCase):
    def test_decode_payload(self):
        b = binascii.unhexlify("05001b07014974616c69616e401e00000000000002")
        self.assertEqual(record.decode_payload(b), [None, "Italian", 7.5, 2])
        b = binascii.unhexlify("040009416161616161616161616161616161616161616161616161616161")
        self.assertEqual(record.decode_payload(b), [None, 1, 'aaaaaaaaaaaaaaaaaaaaaaaaaa'])

    def test_encode_list(self):
        self.assertEqual(
            record.pack_value_list([None, "Italian", 7.5, 2]),
            bytearray(binascii.unhexlify("05001b07014974616c69616e401e00000000000002"))
        )
        self.assertEqual(
            record.pack_value_list([None, 1, 'aaaaaaaaaaaaaaaaaaaaaaaaaa']),
            bytearray(binascii.unhexlify("040009416161616161616161616161616161616161616161616161616161"))
        )

    def test_dict_to_value_list(self):
        test = sqliteio.open("testdata/test.sqlite")
        table_schema = test.tables.get("test_table")
        self.assertEqual(
            table_schema.dict_to_value_list({
                'a': None,
                'b': 'D',
                'c': 4,
                'd': 1.23,
                'e': 1.23,
                'w': b'd',
                'x': '1967-08-11',
                'y': '12:34:45',
                'z': '1967-08-11 12:34:45',
            }),
            (None, ['D', 4, 1.23, 1.23, None, b'd', '1967-08-11', '12:34:45', '1967-08-11 12:34:45'])
        )
        self.assertEqual(
            table_schema.dict_to_value_list({
                'a': 4,
                'b': 'D',
                'c': 4,
                'd': 1.23,
                'e': 1.23,
                'w': b'd',
                'x': '1967-08-11',
                'y': '12:34:45',
                'z': '1967-08-11 12:34:45',
            }),
            (4, ['D', 4, 1.23, 1.23, None, b'd', '1967-08-11', '12:34:45', '1967-08-11 12:34:45'])
        )

        test.close()


class TestPager(unittest.TestCase):
    def test_header(self):
        database = sqliteio.open("testdata/test.sqlite")
        database.pager.pgno_first_freelist_trunk = 2
        self.assertEqual(database.pager.pgno_first_freelist_trunk, 2)
        database.close()


class TestBase(unittest.TestCase):
    def assertEqualDB(self, database1, database2):
        self.assertEqual(database1.pager.max_pgno, database1.pager.max_pgno)
        for i in range(2, database1.pager.max_pgno+1):
            page1 = database1.pager.get_page(i)
            page2 = database2.pager.get_page(i)
            node1 = page1.get_node()
            node2 = page2.get_node()
            if node1 and node2:
                if node1 != node2:
                    page1._dump()
                    node1._dump()
                    page2._dump()
                    node2._dump()
                self.assertEqual(node1, node2)
            else:
                if page1.data != page2.data:
                    page1._dump()
                    page2._dump()
                self.assertEqual(page1.data, page2.data)


class TestNode(TestBase):
    def test_split_table_leaf(self):
        database = sqliteio.open("testdata/test.sqlite")

        # split leaf table node
        table_schema = database.tables.get("test_table")
        ancestors, leaf, cell_index, found = database.pager.find_rowid_table_path(table_schema.pgno, 1)
        self.assertEqual(cell_index, 0)
        self.assertEqual(found, True)
        self.assertEqual(leaf.number_of_cells, 2)

        new_node = leaf.split_by_index(cell_index)
        self.assertEqual(leaf.number_of_cells, 0)
        self.assertEqual(new_node.number_of_cells, 2)

        database.rollback()
        ancestors, leaf, cell_index, found = database.pager.find_rowid_table_path(table_schema.pgno, 1)

        new_node = leaf.split_by_index(1)
        self.assertEqual(leaf.number_of_cells, 1)
        self.assertEqual(new_node.number_of_cells, 1)

        database.close()

    def test_split_index_leaf(self):
        database = sqliteio.open("testdata/many_record15.sqlite")
        index_leaf = database.pager.get_page(4).get_node()

        self.assertEqual(index_leaf.number_of_cells, 15)

        new_leaf, interior_cell_block = index_leaf.split_by_median()

        self.assertEqual(index_leaf.number_of_cells, 8)
        self.assertEqual(new_leaf.number_of_cells, 6)
        self.assertEqual(
            binascii.hexlify(interior_cell_block).decode('ascii'),
            "1e034101616161616161616161616161616161616161616161616161616109"
        )
        database.close()

    def test_find_rowid_index_path(self):
        database = sqliteio.open("testdata/many_record333.sqlite")
        index_interior = database.pager.get_page(4).get_node()
        index_ancestors, index_leaf, index_leaf_cell_index, found = index_interior.find_rowid_index_path(
            ['aaaaaaaaaaaaaaaaaaaaaaaaaa'],
            334,
            [-1],
            [],
            True
        )
        self.assertEqual([a.pgno for a in index_ancestors], [4, 52])
        self.assertEqual(index_leaf.pgno, 78)
        self.assertEqual(index_leaf_cell_index, 14)
        self.assertEqual(found, False)

        index_ancestors, index_leaf, index_leaf_cell_index, found = index_interior.find_rowid_index_path(
            ['abcdefghijklmnopqrstuvwxyz'],
            334,
            [-1],
            [],
            True
        )
        self.assertEqual([a.pgno for a in index_ancestors], [4, 51])
        self.assertEqual(index_leaf.pgno, 8)
        self.assertEqual(index_leaf_cell_index, 0)
        self.assertEqual(found, False)
        database.close()


class TestBasic(TestBase):

    def test_test(self):
        database = sqliteio.open("testdata/test.sqlite")

        # table schema
        table_schema = database.table_schema("test_table")
        self.assertEqual(
            [c.name for c in table_schema.columns],
            ["b", "c", "d", "e", "a", "w", "x", "y", "z"]
        )
        self.assertEqual(
            [c.name for c in table_schema.primary_keys],
            ["a"]
        )
        # fetch_all
        self.assertEqual(
            list(database.fetch_all("test_table")), [
                (1, {'a': 1, 'b': 'A', 'c': 1, 'd': 1.23, 'e': 1.23, 'w': b'a' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (2, {'a': 2, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'b' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (3, {'a': 3, 'b': 'C', 'c': 3, 'd': 1.23, 'e': 1.23, 'w': b'c' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (4, {'a': 4, 'b': 'D', 'c': 4, 'd': 1.23, 'e': 1.23, 'w': b'd', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
            ]
        )

        # get_by_rowid
        self.assertEqual(
            database.get_by_rowid("test_table", 1),
            (1, {'a': 1, 'b': 'A', 'c': 1, 'd': 1.23, 'e': 1.23, 'w': b'a' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'})
        )
        self.assertEqual(
            database.get_by_rowid("test_table", 2),
            (2, {'a': 2, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'b' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
        )
        self.assertEqual(
            database.get_by_rowid("test_table", 3),
            (3, {'a': 3, 'b': 'C', 'c': 3, 'd': 1.23, 'e': 1.23, 'w': b'c' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
        )
        self.assertEqual(
            database.get_by_rowid("test_table", 4),
            (4, {'a': 4, 'b': 'D', 'c': 4, 'd': 1.23, 'e': 1.23, 'w': b'd', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
        )
        self.assertEqual(database.get_by_rowid("test_table", 5), None)

        # get_by_pk
        self.assertEqual(
            database.get_by_pk("test_table", 1),
            (1, {'a': 1, 'b': 'A', 'c': 1, 'd': 1.23, 'e': 1.23, 'w': b'a' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'})
        )
        self.assertEqual(database.get_by_pk("test_table", 5), None)

        # filter_by_index
        index_schema = database.get_index_schema_by_name("test_idx_b_c")
        self.assertEqual(
            list(database.filter_by_index(index_schema, {"b": "A", "c": 1})), [
                (1, {'a': 1, 'b': 'A', 'c': 1, 'd': 1.23, 'e': 1.23, 'w': b'a' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
            ]
        )

        index_schema = database.get_index_schema_by_name("test_idx_b_c")
        with self.assertRaises(ValueError):
            list(database.filter_by_index(index_schema, {"b": "A"}))

        # find_rowid_table_path()
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 1)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 4, 0, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 2)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 4, 1, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 3)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 5, 0, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 4)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 5, 1, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 5)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 5, 2, False))

        # find_rowid_index_path
        index_schema = database.get_index_schema_by_name("test_idx_b_c")
        ancestors, leaf, pos, found = database.pager.find_rowid_index_path(
            index_schema.pgno, ['A', 1], 1, index_schema.orders, True
        )
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 3, 3, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_index_path(
            index_schema.pgno, ['A', 1], 2, index_schema.orders, True
        )
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 3, 4, False))
        ancestors, leaf, pos, found = database.pager.find_rowid_index_path(
            index_schema.pgno, ['E', 5], 5, index_schema.orders, True
        )
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 3, 0, False))

        database.close()

    def test_test0(self):
        database = sqliteio.open("testdata/test0.sqlite")

        # fetch_all
        self.assertEqual(list(database.fetch_all("test_table")), [])

        database.close()

    def test_test1(self):
        database = sqliteio.open("testdata/test1.sqlite")

        # fetch_all
        self.assertEqual(
            list(database.fetch_all("test_table")), [
                (2, {'a': 2, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'b' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (3, {'a': 3, 'b': 'C', 'c': 3, 'd': 1.23, 'e': 1.23, 'w': b'c' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (4, {'a': 4, 'b': 'D', 'c': 4, 'd': 1.23, 'e': 1.23, 'w': b'd', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
            ]
        )

        table_schema = database.table_schema("test_table")
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 1)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 4, 0, False))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 2)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 4, 0, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 3)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 5, 0, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 4)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 5, 1, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 5)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 5, 2, False))

        # find_rowid_index_path
        index_schema = database.get_index_schema_by_name("test_idx_b_c")
        ancestors, leaf, pos, found = database.pager.find_rowid_index_path(
            index_schema.pgno, ['A', 1], 1, index_schema.orders, True
        )
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 3, 3, False))
        ancestors, leaf, pos, found = database.pager.find_rowid_index_path(
            index_schema.pgno, ['B', 2], 2, index_schema.orders, True
        )
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 3, 2, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_index_path(
            index_schema.pgno, ['C', 3], 3, index_schema.orders, True
        )
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 3, 1, True))

        database.close()

    def test_test2(self):
        database = sqliteio.open("testdata/test2.sqlite")

        # fetch_all
        self.assertEqual(
            list(database.fetch_all("test_table")), [
                (1, {'a': 1, 'b': 'A', 'c': 1, 'd': 1.23, 'e': 1.23, 'w': b'a' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (3, {'a': 3, 'b': 'C', 'c': 3, 'd': 1.23, 'e': 1.23, 'w': b'c' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (4, {'a': 4, 'b': 'D', 'c': 4, 'd': 1.23, 'e': 1.23, 'w': b'd', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
            ]
        )

        table_schema = database.table_schema("test_table")
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 1)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 4, 0, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 2)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 4, 1, False))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 3)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 5, 0, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 4)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 5, 1, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 5)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 5, 2, False))

        # find_rowid_index_path
        index_schema = database.get_index_schema_by_name("test_idx_b_c")
        ancestors, leaf, pos, found = database.pager.find_rowid_index_path(
            index_schema.pgno, ['A', 1], 1, index_schema.orders, True
        )
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 3, 2, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_index_path(
            index_schema.pgno, ['B', 2], 2, index_schema.orders, True
        )
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 3, 2, False))
        ancestors, leaf, pos, found = database.pager.find_rowid_index_path(
            index_schema.pgno, ['C', 3], 3, index_schema.orders, True
        )
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 3, 1, True))

        database.close()

    def test_test4(self):
        database = sqliteio.open("testdata/test4.sqlite")

        # fetch_all
        self.assertEqual(
            list(database.fetch_all("test_table")), [
                (1, {'a': 1, 'b': 'A', 'c': 1, 'd': 1.23, 'e': 1.23, 'w': b'a' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (2, {'a': 2, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'b' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (3, {'a': 3, 'b': 'C', 'c': 3, 'd': 1.23, 'e': 1.23, 'w': b'c' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
            ]
        )

        table_schema = database.table_schema("test_table")
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 1)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 4, 0, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 2)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 4, 1, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 3)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 5, 0, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 4)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 5, 1, False))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 5)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 5, 1, False))

        database.close()

    def test_test13(self):
        database = sqliteio.open("testdata/test13.sqlite")

        # fetch_all
        self.assertEqual(
            list(database.fetch_all("test_table")), [
                (2, {'a': 2, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'b' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (4, {'a': 4, 'b': 'D', 'c': 4, 'd': 1.23, 'e': 1.23, 'w': b'd', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
            ]
        )

        table_schema = database.table_schema("test_table")
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 1)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 2, 0, False))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 2)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 2, 0, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 3)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 2, 1, False))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 4)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 2, 1, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 5)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 2, 2, False))

        database.close()

    def test_test23(self):
        database = sqliteio.open("testdata/test23.sqlite")

        # fetch_all
        self.assertEqual(
            list(database.fetch_all("test_table")), [
                (1, {'a': 1, 'b': 'A', 'c': 1, 'd': 1.23, 'e': 1.23, 'w': b'a' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (4, {'a': 4, 'b': 'D', 'c': 4, 'd': 1.23, 'e': 1.23, 'w': b'd', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
            ]
        )

        table_schema = database.table_schema("test_table")
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 1)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 2, 0, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 2)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 2, 1, False))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 3)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 2, 1, False))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 4)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 2, 1, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 5)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 2, 2, False))

        database.close()

    def test_test24(self):
        database = sqliteio.open("testdata/test24.sqlite")

        # fetch_all
        self.assertEqual(
            list(database.fetch_all("test_table")), [
                (1, {'a': 1, 'b': 'A', 'c': 1, 'd': 1.23, 'e': 1.23, 'w': b'a' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (3, {'a': 3, 'b': 'C', 'c': 3, 'd': 1.23, 'e': 1.23, 'w': b'c' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
            ]
        )

        table_schema = database.table_schema("test_table")
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 1)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 4, 0, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 2)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 4, 1, False))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 3)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 5, 0, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 4)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 5, 1, False))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 5)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2], 5, 1, False))

        database.close()

    def test_test1234(self):
        database = sqliteio.open("testdata/test1234.sqlite")

        # fetch_all
        self.assertEqual(list(database.fetch_all("test_table")), [])

        table_schema = database.table_schema("test_table")
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 1)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 2, 0, False))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 2)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 2, 0, False))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 3)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 2, 0, False))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 4)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 2, 0, False))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 5)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([], 2, 0, False))

        database.close()

    def test_no_interior(self):
        database = sqliteio.open("testdata/no_interior.sqlite")

        # fetch_all
        self.assertEqual(
            list(database.fetch_all("test_table")), [
                (1, {'a': 1, 'b': 'A', 'c': 1, 'd': 1.23, 'e': 1.23, 'w': b'a', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (2, {'a': 2, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'bb', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (3, {'a': 3, 'b': 'C', 'c': 3, 'd': 1.23, 'e': 1.23, 'w': b'ccc', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (4, {'a': 4, 'b': 'D', 'c': 4, 'd': 1.23, 'e': 1.23, 'w': b'dddd', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
            ]
        )

        database.delete_by_rowid("test_table", 1)

        database.close()

    def test_no_interior1(self):
        database = sqliteio.open("testdata/no_interior1.sqlite")

        # fetch_all
        self.assertEqual(
            list(database.fetch_all("test_table")), [
                (2, {'a': 2, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'bb', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (3, {'a': 3, 'b': 'C', 'c': 3, 'd': 1.23, 'e': 1.23, 'w': b'ccc', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (4, {'a': 4, 'b': 'D', 'c': 4, 'd': 1.23, 'e': 1.23, 'w': b'dddd', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
            ]
        )
        database.close()

    def test_no_interior2(self):
        database = sqliteio.open("testdata/no_interior2.sqlite")

        # fetch_all
        self.assertEqual(
            list(database.fetch_all("test_table")), [
                (1, {'a': 1, 'b': 'A', 'c': 1, 'd': 1.23, 'e': 1.23, 'w': b'a', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (3, {'a': 3, 'b': 'C', 'c': 3, 'd': 1.23, 'e': 1.23, 'w': b'ccc', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (4, {'a': 4, 'b': 'D', 'c': 4, 'd': 1.23, 'e': 1.23, 'w': b'dddd', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
            ]
        )
        database.close()

    def test_no_interior3(self):
        database = sqliteio.open("testdata/no_interior3.sqlite")

        # fetch_all
        self.assertEqual(
            list(database.fetch_all("test_table")), [
                (1, {'a': 1, 'b': 'A', 'c': 1, 'd': 1.23, 'e': 1.23, 'w': b'a', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (2, {'a': 2, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'bb', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (4, {'a': 4, 'b': 'D', 'c': 4, 'd': 1.23, 'e': 1.23, 'w': b'dddd', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
            ]
        )
        database.close()

    def test_no_interior4(self):
        database = sqliteio.open("testdata/no_interior4.sqlite")

        # fetch_all
        self.assertEqual(
            list(database.fetch_all("test_table")), [
                (1, {'a': 1, 'b': 'A', 'c': 1, 'd': 1.23, 'e': 1.23, 'w': b'a', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (2, {'a': 2, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'bb', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (3, {'a': 3, 'b': 'C', 'c': 3, 'd': 1.23, 'e': 1.23, 'w': b'ccc', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
            ]
        )
        database.close()

    def test_no_interior24(self):
        database = sqliteio.open("testdata/no_interior24.sqlite")

        # fetch_all
        self.assertEqual(
            list(database.fetch_all("test_table")), [
                (1, {'a': 1, 'b': 'A', 'c': 1, 'd': 1.23, 'e': 1.23, 'w': b'a', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (3, {'a': 3, 'b': 'C', 'c': 3, 'd': 1.23, 'e': 1.23, 'w': b'ccc', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
            ]
        )
        database.close()

    def test_no_interior13(self):
        database = sqliteio.open("testdata/no_interior13.sqlite")

        # fetch_all
        self.assertEqual(
            list(database.fetch_all("test_table")), [
                (2, {'a': 2, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'bb', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (4, {'a': 4, 'b': 'D', 'c': 4, 'd': 1.23, 'e': 1.23, 'w': b'dddd', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
            ]
        )
        database.close()

    def test_no_interior12(self):
        database = sqliteio.open("testdata/no_interior12.sqlite")

        # fetch_all
        self.assertEqual(
            list(database.fetch_all("test_table")), [
                (3, {'a': 3, 'b': 'C', 'c': 3, 'd': 1.23, 'e': 1.23, 'w': b'ccc', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (4, {'a': 4, 'b': 'D', 'c': 4, 'd': 1.23, 'e': 1.23, 'w': b'dddd', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
            ]
        )
        database.close()

    def test_no_interior1234(self):
        database = sqliteio.open("testdata/no_interior1234.sqlite")

        # fetch_all
        self.assertEqual(list(database.fetch_all("test_table")), [])
        database.close()

    def test_large_row(self):
        database = sqliteio.open("testdata/large_row.sqlite")

        # fetch_all
        self.assertEqual(
            list(database.fetch_all("test_table")), [
                (1, {'a': 1, 'b': 'A', 'c': 1, 'd': 1.23, 'e': 1.23, 'w': b'a' * 500, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (2, {'a': 2, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'b' * 1000, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (3, {'a': 3, 'b': 'C', 'c': 3, 'd': 1.23, 'e': 1.23, 'w': b'c' * 1500, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
            ]
        )
        database.close()

    def test_large_row0(self):
        database = sqliteio.open("testdata/large_row0.sqlite")

        # fetch_all
        self.assertEqual(list(database.fetch_all("test_table")), [])

        database.close()

    def test_large_row1(self):
        database = sqliteio.open("testdata/large_row1.sqlite")

        # fetch_all
        self.assertEqual(
            list(database.fetch_all("test_table")), [
                (1, {'a': 1, 'b': 'A', 'c': 1, 'd': 1.23, 'e': 1.23, 'w': b'a' * 500, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (3, {'a': 3, 'b': 'C', 'c': 3, 'd': 1.23, 'e': 1.23, 'w': b'c' * 1500, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
            ]
        )
        database.close()

    def test_without_rowid(self):
        database = sqliteio.open("testdata/without_rowid.sqlite")

        table_schema = database.table_schema("without_rowid_table")
        self.assertEqual(
            [c.name for c in table_schema.columns],
            ["a", "b", "c", "d", "e", "w", "x", "y", "z"]
        )
        self.assertEqual(
            [c.name for c in table_schema.primary_keys],
            ["a"]
        )
        self.assertEqual(
            list(database.fetch_all("without_rowid_table")), [
                (None, {'a': 10, 'b': 'A', 'c': 1, 'd': 1.23, 'e': 1.23, 'w': b'bin', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (None, {'a': 20, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'bin', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (None, {'a': 30, 'b': 'C', 'c': 3, 'd': 1.23, 'e': 1.23, 'w': b'bin', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
            ]
        )

        # get_by_pk
        self.assertEqual(
            database.get_by_pk("without_rowid_table", 10),
            (None, {'a': 10, 'b': 'A', 'c': 1, 'd': 1.23, 'e': 1.23, 'w': b'bin', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'})
        )
        self.assertEqual(
            database.get_by_pk("without_rowid_table", 20),
            (None, {'a': 20, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'bin', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'})
        )
        self.assertEqual(
            database.get_by_pk("without_rowid_table", 30),
            (None, {'a': 30, 'b': 'C', 'c': 3, 'd': 1.23, 'e': 1.23, 'w': b'bin', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'})
        )
        self.assertEqual(database.get_by_pk("without_rowid_table", 1), None)
        database.close()

    def test_pk_fk_table(self):
        database = sqliteio.open("testdata/pk_fk.sqlite")
        self.assertEqual(
            list(database.fetch_all("base_table")), [
                (10, {'a': 10, 'b': 'A', 'c': 1, 'd': 1.23, 'e': 1.23, 'w': b'bin', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (20, {'a': 20, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'bin', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (30, {'a': 30, 'b': 'C', 'c': 3, 'd': 1.23, 'e': 1.23, 'w': b'bin', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
            ]
        )
        database.close()

    def test_str_pk(self):
        database = sqliteio.open("testdata/str_pk.sqlite")
        self.assertEqual(
            list(database.fetch_all("str_pk_table")), [
                (1, {'s': "A", 't': "a"}),
                (2, {'s': "B", 't': "b"}),
                (3, {'s': "C", 't': "c"}),
            ]
        )

        # get_by_rowid
        self.assertEqual(database.get_by_rowid("str_pk_table", 1), (1, {'s': "A", 't': "a"}))
        self.assertEqual(database.get_by_rowid("str_pk_table", 2), (2, {'s': "B", 't': "b"}))
        self.assertEqual(database.get_by_rowid("str_pk_table", 3), (3, {'s': "C", 't': "c"}))
        self.assertEqual(database.get_by_rowid("str_pk_table", 4), None)

        # get_by_pk
        self.assertEqual(database.get_by_pk("str_pk_table", "A"), (1, {'s': "A", 't': "a"}))
        self.assertEqual(database.get_by_pk("str_pk_table", "B"), (2, {'s': "B", 't': "b"}))
        self.assertEqual(database.get_by_pk("str_pk_table", "C"), (3, {'s': "C", 't': "c"}))
        self.assertEqual(database.get_by_pk("str_pk_table", "D"), None)

        database.close()

    def test_multi_pk(self):
        database = sqliteio.open("testdata/multi_pk.sqlite")
        table_schema = database.table_schema("multi_pk_table")
        self.assertEqual(
            [c.name for c in table_schema.columns],
            ["a", "b"]
        )
        self.assertEqual(
            [c.name for c in table_schema.primary_keys],
            ["a", "b"]
        )
        self.assertEqual(
            list(database.fetch_all("multi_pk_table")), [
                (1, {'a': 1, 'b': "A"}),
                (2, {'a': 2, 'b': "A"}),
                (3, {'a': 3, 'b': "A"}),
            ]
        )

        # get_by_rowid
        self.assertEqual(database.get_by_rowid("multi_pk_table", 1), (1, {'a': 1, 'b': "A"}))
        self.assertEqual(database.get_by_rowid("multi_pk_table", 2), (2, {'a': 2, 'b': "A"}))
        self.assertEqual(database.get_by_rowid("multi_pk_table", 3), (3, {'a': 3, 'b': "A"}))
        self.assertEqual(database.get_by_rowid("multi_pk_table", 4), None)

        # get_by_pk
        self.assertEqual(database.get_by_pk("multi_pk_table", [1, "A"]), (1, {'a': 1, 'b': "A"}))
        self.assertEqual(database.get_by_pk("multi_pk_table", [2, "A"]), (2, {'a': 2, 'b': "A"}))
        self.assertEqual(database.get_by_pk("multi_pk_table", [3, "A"]), (3, {'a': 3, 'b': "A"}))
        self.assertEqual(database.get_by_pk("multi_pk_table", [4, "A"]), None)

        database.close()

    def test_many_record(self):
        database = sqliteio.open("testdata/many_record.sqlite")
        for i in range(1, 334):
            self.assertEqual(
                database.get_by_rowid("many_record_table", i),
                (i, {'a': i, 'b': i, 'c': "aaaaaaaaaaaaaaaaaaaaaaaaaa"})
            )
        for i in range(334, 667):
            self.assertEqual(
                database.get_by_rowid("many_record_table", i),
                (i, {'a': i, 'b': i, 'c': "abcdefghijklmnopqrstuvwxyz"})
            )
        for i in range(667, 1000):
            self.assertEqual(
                database.get_by_rowid("many_record_table", i),
                (i, {'a': i, 'b': i, 'c': "zzzzzzzzzzzzzzzzzzzzzzzzzz"})
            )
        self.assertEqual(database.get_by_rowid("many_record_table", 1000), None)

        table_schema = database.table_schema("many_record_table")

        # find_rowid_table_path()
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 1)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2, 203], 6, 0, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 500)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2, 204], 119, 9, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 997)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2, 204], 239, 12, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 998)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2, 204], 241, 0, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 999)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2, 204], 241, 1, True))
        ancestors, leaf, pos, found = database.pager.find_rowid_table_path(table_schema.pgno, 1000)
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([2, 204], 241, 2, False))

        # find_rowid_index_path
        # index record on IndexInteriorRecord
        index_schema = database.get_index_schema_by_name("many_record_idx_c")
        ancestors, leaf, pos, found = database.pager.find_rowid_index_path(
            index_schema.pgno, ['zzzzzzzzzzzzzzzzzzzzzzzzzz'], 723, index_schema.orders, True
        )
        self.assertEqual(([p.page.pgno for p in ancestors], leaf, pos, found), ([3], None, 3, True))
        # index record on IndexLeafRecord
        ancestors, leaf, pos, found = database.pager.find_rowid_index_path(
            index_schema.pgno, ['zzzzzzzzzzzzzzzzzzzzzzzzzz'], 734, index_schema.orders, True
        )
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([3, 189], 175, 10, True))

        ancestors, leaf, pos, found = database.pager.find_rowid_index_path(
            index_schema.pgno, ['AAAAAAAAAAAAAAAAAAAAAAAAAA'], 1, index_schema.orders, True
        )
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([3, 54], 10, 0, False))

        index_schema = database.get_index_schema_by_name("many_record_idx_c_desc")
        ancestors, leaf, pos, found = database.pager.find_rowid_index_path(
            index_schema.pgno, ['zzzzzzzzzzzzzzzzzzzzzzzzzz'], 723, index_schema.orders, True
        )
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([4, 51], 170, 0, True))
        # index record on IndexLeafRecord
        ancestors, leaf, pos, found = database.pager.find_rowid_index_path(
            index_schema.pgno, ['zzzzzzzzzzzzzzzzzzzzzzzzzz'], 734, index_schema.orders, True
        )
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([4, 51], 170, 11, True))

        ancestors, leaf, pos, found = database.pager.find_rowid_index_path(
            index_schema.pgno, ['AAAAAAAAAAAAAAAAAAAAAAAAAA'], 1, index_schema.orders, True
        )
        self.assertEqual(([p.page.pgno for p in ancestors], leaf.page.pgno, pos, found), ([4, 142], 78, 14, False))

        database.close()

    def test_without_rowid_many_record(self):
        database = sqliteio.open("testdata/without_rowid_many_record.sqlite")
        for i in range(1, 1000):
            self.assertEqual(
                database.get_by_pk("without_rowid_many_record_table", i),
                (None, {'a': i, 'b': "abcdefghijklmnopqrstuvwxyz"})
            )
        self.assertEqual(database.get_by_pk("without_rowid_many_record_table", 1000), None)

        database.close()

    def test_str_pk_many_record(self):
        database = sqliteio.open("testdata/str_pk_many_record.sqlite")
        for i in range(1, 500):
            self.assertEqual(
                database.get_by_pk("str_pk_many_record_table", str(i))[1],
                {'s': str(i), 't': str(i)}
            )
        database.close()

    def test_multi_pk_many_record(self):
        database = sqliteio.open("testdata/multi_pk_many_record.sqlite")
        for i in range(1, 1000):
            self.assertEqual(
                database.get_by_pk("multi_pk_many_record_table", [i, "aaaaaaaaaaaaaaaaaaaa"])[1],
                {'a': i, 'b': "aaaaaaaaaaaaaaaaaaaa"}
            )
        database.close()


class TestCell(TestBase):
    def test_first_payload_len(self):
        test = sqliteio.open("testdata/test.sqlite")

        test_ins_shortshort = sqliteio.open("testdata/test_ins_shortshort.sqlite")
        test.insert(
            "test_table", [
                {'a': None, 'b': 'E', 'c': 5, 'd': 1.23, 'e': 1.23, 'w': b'e' * 987, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'},
            ]
        )
        self.assertEqualDB(test, test_ins_shortshort)
        test_ins_shortshort.close()
        test.rollback()

        test_ins_short = sqliteio.open("testdata/test_ins_short.sqlite")
        test.insert(
            "test_table", [
                {'a': None, 'b': 'E', 'c': 5, 'd': 1.23, 'e': 1.23, 'w': b'e' * 988, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'},
            ]
        )
        self.assertEqualDB(test, test_ins_short)
        test_ins_short.close()
        test.rollback()

        test_ins_short1 = sqliteio.open("testdata/test_ins_short1.sqlite")
        test.insert(
            "test_table", [
                {'a': None, 'b': 'E', 'c': 5, 'd': 1.23, 'e': 1.23, 'w': b'e' * 989, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'},
            ]
        )
        self.assertEqualDB(test, test_ins_short1)
        test_ins_short1.close()
        test.rollback()

        test_ins_short2 = sqliteio.open("testdata/test_ins_short2.sqlite")
        test.insert(
            "test_table", [
                {'a': None, 'b': 'E', 'c': 5, 'd': 1.23, 'e': 1.23, 'w': b'e' * 990, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'},
            ]
        )
        self.assertEqualDB(test, test_ins_short2)
        test_ins_short2.close()
        test.rollback()

        test_ins_middle = sqliteio.open("testdata/test_ins_middle.sqlite")
        test.insert(
            "test_table", [
                {'a': None, 'b': 'E', 'c': 5, 'd': 1.23, 'e': 1.23, 'w': b'e' * 1000, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'},
            ]
        )
        self.assertEqualDB(test, test_ins_middle)
        test_ins_middle.close()
        test.rollback()

        test_ins_long = sqliteio.open("testdata/test_ins_long.sqlite")
        test.insert(
            "test_table", [
                {'a': None, 'b': 'E', 'c': 5, 'd': 1.23, 'e': 1.23, 'w': b'e' * 1154, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'},
            ]
        )
        self.assertEqualDB(test, test_ins_long)
        test_ins_long.close()
        test.rollback()

        test_ins_longlong = sqliteio.open("testdata/test_ins_longlong.sqlite")
        test.insert(
            "test_table", [
                {'a': None, 'b': 'E', 'c': 5, 'd': 1.23, 'e': 1.23, 'w': b'e' * 1155, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'},
            ]
        )
        self.assertEqualDB(test, test_ins_longlong)
        test_ins_longlong.close()
        test.rollback()

        test.close()


class TestInsert(TestBase):
    def test_next_rowid(self):
        test = sqliteio.open("testdata/test.sqlite")
        table_schema = test.table_schema("test_table")
        self.assertEqual(test._get_next_rowid(table_schema), 5)
        test.close()

        test0 = sqliteio.open("testdata/test0.sqlite")
        table_schema = test0.table_schema("test_table")
        self.assertEqual(test0._get_next_rowid(table_schema), 1)
        test0.close()

        many_record = sqliteio.open("testdata/many_record.sqlite")
        table_schema = many_record.table_schema("many_record_table")
        self.assertEqual(many_record._get_next_rowid(table_schema), 1000)
        many_record.close()

    def test_insert_no_interior(self):
        no_interior = sqliteio.open("testdata/no_interior.sqlite")
        no_interior_ins567 = sqliteio.open("testdata/no_interior_ins567.sqlite")
        no_interior.insert(
            "test_table", [
                {'a': None, 'b': 'E', 'c': 5, 'd': 1.23, 'e': 1.23, 'w': b'e', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'},
                {'a': None, 'b': 'F', 'c': 6, 'd': 1.23, 'e': 1.23, 'w': b'f', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'},
                {'a': None, 'b': 'G', 'c': 7, 'd': 1.23, 'e': 1.23, 'w': b'g', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'},
            ]
        )
        self.assertEqualDB(no_interior, no_interior_ins567)
        no_interior_ins567.close()

        no_interior_ins5678 = sqliteio.open("testdata/no_interior_ins5678.sqlite")
        no_interior.insert(
            "test_table", [
                {'a': None, 'b': 'H', 'c': 8, 'd': 1.23, 'e': 1.23, 'w': b'h', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'},
            ]
        )
        self.assertEqualDB(no_interior, no_interior_ins5678)
        no_interior_ins5678.close()
        no_interior.close()

        no_interior24 = sqliteio.open("testdata/no_interior24.sqlite")
        no_interior24.insert(
            "test_table", [
                {'a': 4, 'b': 'D', 'c': 4, 'd': 1.23, 'e': 1.23, 'w': b'dddd', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'},
            ]
        )
        no_interior24.insert(
            "test_table", [
                {'a': 2, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'bb', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'},
            ]
        )
        no_interior24_ins42 = sqliteio.open("testdata/no_interior24_ins42.sqlite")
        self.assertEqualDB(no_interior24, no_interior24_ins42)
        no_interior24_ins42.close()
        no_interior24.close()

    def test_insert_no_large_row(self):
        test = sqliteio.open("testdata/test.sqlite")

        test_ins5 = sqliteio.open("testdata/test_ins5.sqlite")
        test.insert(
            "test_table", [
                {'a': None, 'b': 'E', 'c': 5, 'd': 1.23, 'e': 1.23, 'w': b'e', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'},
            ]
        )
        self.assertEqualDB(test, test_ins5)
        test_ins5.close()

        test_ins567 = sqliteio.open("testdata/test_ins567.sqlite")
        test.insert(
            "test_table", [
                {'a': None, 'b': 'F', 'c': 6, 'd': 1.23, 'e': 1.23, 'w': b'f', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'},
                {'a': None, 'b': 'G', 'c': 7, 'd': 1.23, 'e': 1.23, 'w': b'g', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'},
            ]
        )
        self.assertEqualDB(test, test_ins567)
        test_ins567.close()

        test_ins5678 = sqliteio.open("testdata/test_ins5678.sqlite")
        test.insert(
            "test_table", [
                {'a': None, 'b': 'H', 'c': 8, 'd': 1.23, 'e': 1.23, 'w': b'h', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'},
            ]
        )
        self.assertEqualDB(test, test_ins5678)
        test_ins5678.close()
        test.close()

        test2 = sqliteio.open("testdata/test2.sqlite")

        test2_ins2 = sqliteio.open("testdata/test2_ins2.sqlite")
        test2.insert(
            "test_table", [
                {'a': None, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'b' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'},
            ]
        )
        self.assertEqualDB(test2, test2_ins2)
        test2_ins2.close()
        test2.close()

        test2 = sqliteio.open("testdata/test2.sqlite")
        test2_ins2_short = sqliteio.open("testdata/test2_ins2_short.sqlite")
        test2.insert(
            "test_table", [
                {'a': None, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'b' * 149, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'},
            ]
        )
        self.assertEqualDB(test2, test2_ins2_short)
        test2_ins2_short.close()
        test2.close()

        test2 = sqliteio.open("testdata/test2.sqlite")
        test2_ins2_long = sqliteio.open("testdata/test2_ins2_long.sqlite")
        test2.insert(
            "test_table", [
                {'a': None, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'b' * 151, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'},
            ]
        )
        self.assertEqualDB(test2, test2_ins2_long)
        test2_ins2_long.close()
        test2.close()

    def test_rollback(self):
        # insert & rollback
        test2_rollback = sqliteio.open("testdata/test2.sqlite")
        test2_rollback.insert(
            "test_table", [
                {'a': None, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'b' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'},
            ]
        )
        test2_rollback.rollback()
        test2_orig = sqliteio.open("testdata/test2.sqlite")
        self.assertEqualDB(test2_rollback, test2_orig)
        test2_rollback.close()
        test2_orig.close()

        # delete & rollback
        no_interior = sqliteio.open("testdata/no_interior.sqlite")
        no_interior.delete_by_rowid("test_table", 1)
        no_interior.rollback()
        no_interior.delete_by_rowid("test_table", 2)
        no_interior2 = sqliteio.open("testdata/no_interior2.sqlite")
        self.assertEqualDB(no_interior, no_interior2)
        no_interior2.close()
        no_interior.close()

    def test_insert_many_record(self):
        many_record_empty = sqliteio.open("testdata/many_record_empty.sqlite")

        many_record15 = sqliteio.open("testdata/many_record15.sqlite")
        for i in range(1, 16):
            many_record_empty.insert(
                "many_record_table",
                [{'a': None, 'b': i, 'c': 'aaaaaaaaaaaaaaaaaaaaaaaaaa'}]
            )
        self.assertEqualDB(many_record_empty, many_record15)
        many_record15.close()
        many_record_empty.rollback()

        many_record16 = sqliteio.open("testdata/many_record16.sqlite")
        for i in range(1, 17):
            many_record_empty.insert(
                "many_record_table",
                [{'a': None, 'b': i, 'c': 'aaaaaaaaaaaaaaaaaaaaaaaaaa'}]
            )
        self.assertEqualDB(many_record_empty, many_record16)
        many_record16.close()
        many_record_empty.rollback()

        many_record = sqliteio.open("testdata/many_record.sqlite")
        for i in range(1, 334):
            many_record_empty.insert(
                "many_record_table",
                [{'a': None, 'b': i, 'c': 'aaaaaaaaaaaaaaaaaaaaaaaaaa'}]
            )
        for i in range(334, 667):
            many_record_empty.insert(
                "many_record_table",
                [{'a': None, 'b': i, 'c': 'abcdefghijklmnopqrstuvwxyz'}]
            )
        for i in range(667, 1000):
            many_record_empty.insert(
                "many_record_table",
                [{'a': None, 'b': i, 'c': 'zzzzzzzzzzzzzzzzzzzzzzzzzz'}]
            )
        # TODO: Comparison with logical data
        # self.assertEqualDB(many_record_empty, many_record)
        many_record.close()
        many_record_empty.close()


class TestDelete(TestBase):
    def test_delete_test(self):
        test = sqliteio.open("testdata/test.sqlite")

        test1 = sqliteio.open("testdata/test1.sqlite")
        test.delete_by_rowid("test_table", 1)
        self.assertEqualDB(test, test1)
        test.rollback()
        test1.close()

        test2 = sqliteio.open("testdata/test2.sqlite")
        test.delete_by_rowid("test_table", 2)
        self.assertEqualDB(test, test2)
        test.rollback()
        test2.close()

        test3 = sqliteio.open("testdata/test3.sqlite")
        test.delete_by_rowid("test_table", 3)
        self.assertEqualDB(test, test3)
        test.rollback()
        test3.close()

        test4 = sqliteio.open("testdata/test4.sqlite")
        test.delete_by_rowid("test_table", 4)
        self.assertEqualDB(test, test4)
        test.rollback()
        test4.close()

        test13 = sqliteio.open("testdata/test13.sqlite")
        test.delete_by_rowid("test_table", 1)
        test.delete_by_rowid("test_table", 3)
        self.assertEqualDB(test, test13)
        test.rollback()
        test13.close()
        test31 = sqliteio.open("testdata/test31.sqlite")
        test.delete_by_rowid("test_table", 3)
        test.delete_by_rowid("test_table", 1)
        self.assertEqual(list(test.fetch_all("test_table")), list(test31.fetch_all("test_table")))
        test.rollback()
        test31.close()

        test23 = sqliteio.open("testdata/test23.sqlite")
        test.delete_by_rowid("test_table", 2)
        test.delete_by_rowid("test_table", 3)
        self.assertEqualDB(test, test23)
        test.rollback()
        test23.close()

        test32 = sqliteio.open("testdata/test32.sqlite")
        test.delete_by_rowid("test_table", 3)
        test.delete_by_rowid("test_table", 2)
        self.assertEqual(list(test.fetch_all("test_table")), list(test32.fetch_all("test_table")))
        test.rollback()
        test32.close()

        test24 = sqliteio.open("testdata/test24.sqlite")
        test.delete_by_rowid("test_table", 2)
        test.delete_by_rowid("test_table", 4)
        self.assertEqual(list(test.fetch_all("test_table")), list(test24.fetch_all("test_table")))
        test.rollback()
        test24.close()

        test42 = sqliteio.open("testdata/test42.sqlite")
        test.delete_by_rowid("test_table", 4)
        test.delete_by_rowid("test_table", 2)
        self.assertEqual(list(test.fetch_all("test_table")), list(test42.fetch_all("test_table")))
        test.rollback()
        test42.close()

        test1234 = sqliteio.open("testdata/test1234.sqlite")
        test.delete_by_rowid("test_table", 1)
        test.delete_by_rowid("test_table", 2)
        test.delete_by_rowid("test_table", 3)
        test.delete_by_rowid("test_table", 4)
        self.assertEqualDB(test, test1234)
        test.rollback()
        test1234.close()

        test.close()

    def test_delete_no_interior(self):
        no_interior = sqliteio.open("testdata/no_interior.sqlite")

        no_interior1 = sqliteio.open("testdata/no_interior1.sqlite")
        no_interior.delete_by_rowid("test_table", 1)
        self.assertEqualDB(no_interior, no_interior1)
        no_interior.rollback()
        no_interior1.close()

        no_interior2 = sqliteio.open("testdata/no_interior2.sqlite")
        no_interior.delete_by_rowid("test_table", 2)
        self.assertEqualDB(no_interior, no_interior2)
        no_interior.rollback()
        no_interior2.close()

        no_interior3 = sqliteio.open("testdata/no_interior3.sqlite")
        no_interior.delete_by_rowid("test_table", 3)
        self.assertEqualDB(no_interior, no_interior3)
        no_interior.rollback()
        no_interior3.close()

        no_interior4 = sqliteio.open("testdata/no_interior4.sqlite")
        no_interior.delete_by_rowid("test_table", 4)
        self.assertEqualDB(no_interior, no_interior4)
        no_interior.rollback()
        no_interior4.close()

        no_interior24 = sqliteio.open("testdata/no_interior24.sqlite")
        no_interior.delete_by_rowid("test_table", 2)
        no_interior.delete_by_rowid("test_table", 4)
        self.assertEqualDB(no_interior, no_interior24)
        no_interior.rollback()
        no_interior24.close()

        no_interior13 = sqliteio.open("testdata/no_interior13.sqlite")
        no_interior.delete_by_rowid("test_table", 1)
        no_interior.delete_by_rowid("test_table", 3)
        self.assertEqualDB(no_interior, no_interior13)
        no_interior.rollback()
        no_interior13.close()

        no_interior12 = sqliteio.open("testdata/no_interior12.sqlite")
        no_interior.delete_by_rowid("test_table", 1)
        no_interior.delete_by_rowid("test_table", 2)
        self.assertEqualDB(no_interior, no_interior12)
        no_interior.rollback()
        no_interior12.close()

        no_interior1234 = sqliteio.open("testdata/no_interior1234.sqlite")
        no_interior.delete_by_rowid("test_table", 1)
        no_interior.delete_by_rowid("test_table", 2)
        no_interior.delete_by_rowid("test_table", 3)
        no_interior.delete_by_rowid("test_table", 4)
        self.assertEqualDB(no_interior, no_interior1234)
        no_interior.rollback()
        no_interior1234.close()

        no_interior.close()

    def test_delete_large_row(self):
        large_row = sqliteio.open("testdata/large_row.sqlite")

        large_row1 = sqliteio.open("testdata/large_row1.sqlite")
        large_row.delete_by_rowid("test_table", 2)
        self.assertEqualDB(large_row, large_row1)
        large_row.rollback()
        large_row1.close()

        large_row.close()


class TestUpdate(TestBase):
    def test_simple_update(self):
        database = sqliteio.open("testdata/test.sqlite")
        database.update_by_rowid(
            "test_table",
            2,
            {'w': b'updated'}
        )
        self.assertEqual(
            database.get_by_rowid("test_table", 2),
            (2, {'a': 2, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'updated', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'})
        )
        database.close()

    def test_rowid_update(self):
        database = sqliteio.open("testdata/test.sqlite")
        with self.assertRaises(ValueError):
            # rowid 1 is already exist
            database.update_by_rowid(
                "test_table",
                2,
                {'a': 1}
            )

        database.update_by_rowid(
            "test_table",
            2,
            {'a': 10}
        )
        self.assertEqual(
            database.get_by_rowid("test_table", 10),
            (10, {'a': 10, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'b' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'})
        )
        database.close()


class TestOpen(TestBase):
    def test_readobly(self):
        fileobj = open("testdata/test.sqlite", "rb")
        test = sqliteio.open(fileobj)
        self.assertEqual(
            list(test.fetch_all("test_table")), [
                (1, {'a': 1, 'b': 'A', 'c': 1, 'd': 1.23, 'e': 1.23, 'w': b'a' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (2, {'a': 2, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'b' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (3, {'a': 3, 'b': 'C', 'c': 3, 'd': 1.23, 'e': 1.23, 'w': b'c' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                (4, {'a': 4, 'b': 'D', 'c': 4, 'd': 1.23, 'e': 1.23, 'w': b'd', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
            ]
        )

        test.delete_by_rowid("test_table", 1)
        with self.assertRaises(Exception):
            test.commit()
        test.close()

    def test_bytesio(self):
        with open("testdata/test.sqlite", "rb") as f:
            bytesio = io.BytesIO(f.read())
            test = sqliteio.open(bytesio)
            self.assertEqual(
                list(test.fetch_all("test_table")), [
                    (1, {'a': 1, 'b': 'A', 'c': 1, 'd': 1.23, 'e': 1.23, 'w': b'a' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                    (2, {'a': 2, 'b': 'B', 'c': 2, 'd': 1.23, 'e': 1.23, 'w': b'b' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                    (3, {'a': 3, 'b': 'C', 'c': 3, 'd': 1.23, 'e': 1.23, 'w': b'c' * 150, 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                    (4, {'a': 4, 'b': 'D', 'c': 4, 'd': 1.23, 'e': 1.23, 'w': b'd', 'x': '1967-08-11', 'y': '12:34:45', 'z': '1967-08-11 12:34:45'}),
                ]
            )
            test.delete_by_rowid("test_table", 1)
            test1 = sqliteio.open("testdata/test1.sqlite")
            self.assertEqualDB(test, test1)
            test1.close()
            test.close()


if __name__ == "__main__":
    unittest.main()
