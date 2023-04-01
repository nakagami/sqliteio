import builtins
from .pager import Pager
from .schema import TableSchema, IndexSchema, ViewSchema
from .btree import TableLeafNode, TableInteriorNode, IndexInteriorNode, swap_node


__all__ = ("Database", "open")


class Database:
    def __init__(self, fileobj):
        self.fileobj = fileobj
        self.pager = Pager(self)
        self.tables = {}
        self.indexes = {}
        self.views = {}
        for _, r in self.pager.records(1):
            # type, name, table_name, pgno, sql
            if r[0] == 'table':
                self.tables[r[2]] = TableSchema(r[1], r[2], r[3], r[4], self)
            elif r[0] == 'index':
                self.indexes.setdefault(r[2], []).append(IndexSchema(r[1], r[2], r[3], r[4], self.tables[r[2]]))
            elif r[0] == 'view':
                self.views[r[2]] = ViewSchema(r[1], r[2], r[3], r[4])

    def __enter__(self):
        return self

    def __exit__(self, exc, value, traceback):
        self.close()

    def _get_primary_key_index(self, table_name):
        for i in self.index_schemas(table_name):
            if i.is_primary_key:
                return i
        return None

    def _get_indexes(self):
        for indexes in self.indexes.values():
            for r in indexes:
                yield r

    def get_index_schema_by_name(self, name):
        "Get IndexSchama by index name"
        for i in self._get_indexes():
            if i.name == name:
                return i
        return None

    def table_schema(self, table_name):
        "TableSchema by table name"
        return self.tables.get(table_name)

    def index_schemas(self, table_name):
        "IndexSchema list by table name"
        return self.indexes.get(table_name)

    def fetch_all(self, table_name):
        "Fetch all table records"
        table_schema = self.table_schema(table_name)
        return self.pager.records(table_schema.pgno, table_schema.row_converter)

    def filter_by_index(self, index_schema, key_dict):
        "Filter by index column and Fetch records"
        key_column_names = [c.name for c in index_schema.columns]
        if set(key_column_names) != set(key_dict.keys()):
            raise ValueError("index columns={}".format(",".join(key_column_names)))
        key_values = [key_dict[k] for k in key_column_names]

        for _, r in self.pager.index_range_records(
            index_schema.pgno,
            key_values, key_values,
            index_schema.orders,
            list(range(len(index_schema.columns)))
        ):
            yield self.get_by_rowid(index_schema.table_name, r[-1])

    def get_by_rowid(self, table_name, rowid):
        "Get table record by rowid"
        table_schema = self.table_schema(table_name)
        try:
            return next(self.pager.rowid_range_records(table_schema.pgno, rowid, rowid, table_schema.row_converter))
        except StopIteration:
            return None

    def get_by_pk(self, table_name, value):
        "Get table record by primary key"
        table_schema = self.table_schema(table_name)
        index_schema = self._get_primary_key_index(table_name)
        if any([c.is_rowid for c in table_schema.columns]):
            try:
                return next(self.pager.rowid_range_records(table_schema.pgno, value, value, table_schema.row_converter))
            except StopIteration:
                return None
        elif table_schema.without_rowid:
            if not isinstance(value, list):
                value = [value]
            g = self.pager.index_range_records(
                table_schema.pgno,
                value, value,
                [1] * len(table_schema.primary_keys),
                list(range(len(table_schema.primary_keys))),
                table_schema.row_converter
            )
            try:
                return next(g)
            except StopIteration:
                return None
        elif index_schema:
            # get by indexed primary key
            if not isinstance(value, list):
                value = [value]
            g = self.pager.index_range_records(
                index_schema.pgno,
                value, value,
                index_schema.orders,
                list(range(len(index_schema.columns))),
            )
            try:
                _, r = next(g)
            except StopIteration:
                return None
            return self.get_by_rowid(table_name, r[-1])

        return None

    def _get_next_rowid(self, table_schema):
        node = self.pager.get_page(table_schema.pgno).get_node()
        assert isinstance(node, (TableLeafNode, TableInteriorNode))
        while isinstance(node, TableInteriorNode):
            node = self.pager.get_page(node.right_most).get_node()
        if len(node.cells) == 0:
            return 1
        return node.cells[-1].rowid + 1

    def _insert1(self, r, table_schema, index_schemas):
        rowid, value_list = table_schema.dict_to_value_list(r)
        if rowid is None:
            rowid = self._get_next_rowid(table_schema)

        table_ancestors, table_leaf, table_leaf_cell_index, found = self.pager.find_rowid_table_path(table_schema.pgno, rowid)

        if found:
            raise ValueError("rowid:{} is exists".format(rowid))

        # Insert record to TableLeafNode
        cell_block = table_leaf.to_cell_block(rowid, value_list)
        cell_index = table_leaf.find_cell_index(rowid)
        if table_leaf.free_cell_size() >= len(cell_block):
            table_leaf.insert(rowid, cell_index, cell_block)
        else:   # overflow table leaf
            new_leaf = table_leaf.split_by_index(cell_index)
            if new_leaf.number_of_cells == 0:
                new_leaf.insert(rowid, cell_index, cell_block)
                if len(table_ancestors) == 0:
                    parent = TableInteriorNode.new_node(self.pager)
                    table_leaf, parent = swap_node(parent, table_leaf)
                    new_leaf, table_leaf = swap_node(table_leaf, new_leaf)
                    parent.right_most = new_leaf.pgno
                    parent.insert_node_index(table_leaf, 0)
                else:
                    parent = table_ancestors[-1]
                    parent.insert_node_after(new_leaf, table_ancestors[:-1], table_leaf)
            else:
                table_leaf.insert(rowid, cell_index, cell_block)

        # Insert index to IndexLeafNode
        for index_schema in reversed(index_schemas):
            key = [r[c.name] for c in index_schema.columns]
            index_ancestors, index_leaf, index_leaf_cell_index, found = self.pager.find_rowid_index_path(
                index_schema.pgno, key, rowid, index_schema.orders, True
            )
            cell_block = index_leaf.to_cell_block(rowid, key)
            cell_index = index_leaf.find_cell_index(key, index_schema)
            if index_leaf.free_cell_size() < len(cell_block):   # overflow index leaf
                new_leaf, interior_cell_block = index_leaf.split_by_median()
                index_leaf.sweep()
                if len(index_ancestors) == 0:
                    parent = IndexInteriorNode.new_node(self.pager)
                    index_leaf, parent = swap_node(parent, index_leaf)
                    new_leaf, index_leaf = swap_node(index_leaf, new_leaf)
                    parent.right_most = new_leaf.pgno
                    parent.insert_cell_block(0, index_leaf.pgno.to_bytes(4, "big") + interior_cell_block)
                    index_leaf = new_leaf
                else:
                    parent = index_ancestors[-1]
                    if parent.free_cell_size() < len(interior_cell_block):  # overflow index interior
                        # split parent index interior and retry to find path
                        parent.split_by_median(index_ancestors[:-1])
                        # TODO: find index_leaf without find_rowid_index_path()
                        index_ancestors, index_leaf, index_leaf_cell_index, found = self.pager.find_rowid_index_path(
                            index_schema.pgno, key, rowid, index_schema.orders, True
                        )
                        cell_block = index_leaf.to_cell_block(rowid, key)
                        cell_index = index_leaf.find_cell_index(key, index_schema)
            index_leaf.insert(rowid, key, cell_index, cell_block)

    def insert(self, table_name, dict_list):
        """insert data
        dict_list is iterator of value dict
        """
        table_schema = self.table_schema(table_name)
        index_schemas = self.index_schemas(table_name)

        for r in dict_list:
            self._insert1(r, table_schema, index_schemas)

    def delete_by_rowid(self, table_name, rowid):
        "delete table record by rowid"
        table_schema = self.table_schema(table_name)
        table_ancestors, table_leaf, table_leaf_cell_index, found = self.pager.find_rowid_table_path(table_schema.pgno, rowid)

        if not found:
            raise ValueError("rowid can't found:{}".format(rowid))
        _, row = table_leaf.record(table_leaf_cell_index, table_schema.row_converter)
        # find related index and remove index
        for index_schema in self.index_schemas(table_name):
            key = [row[c.name] for c in index_schema.columns]
            index_ancestors, index_leaf, index_leaf_cell_index, found = self.pager.find_rowid_index_path(
                index_schema.pgno, key, rowid, index_schema.orders, False
            )
            if found:
                index_leaf.delete(index_leaf_cell_index)
        # remove record
        table_leaf.delete(table_leaf_cell_index)
        if table_ancestors:
            table_ancestors[-1].merge_children()

    def commit(self):
        "Save cache page data to storage"
        self.pager.flush()

    def rollback(self):
        "Rollback dirty pages"
        self.pager.rollback()

    def close(self):
        self.pager.close()


def open(fileobj):
    if isinstance(fileobj, str):
        fileobj = builtins.open(fileobj, "rb+")
    return Database(fileobj)
