import binascii
from .btree import (
    BTREE_PAGE_TYPE_LEAF_TABLE,
    BTREE_PAGE_TYPE_INTERIOR_TABLE,
    BTREE_PAGE_TYPE_LEAF_INDEX,
    BTREE_PAGE_TYPE_INTERIOR_INDEX,
    BTREE_PAGE_TYPE_FREE_PAGE,
    BTREE_PAGE_TYPE_RAW_PAGE,
    TableLeafNode,
    TableInteriorNode,
    IndexLeafNode,
    IndexInteriorNode,
    FreePage,
    RawPage,
)


__all__ = ("Page", "Pager")


class Page:
    def __init__(self, pager, pgno, data, page_type, is_dirty):
        self.pager = pager
        self.pgno = pgno
        self.data = bytearray(data)
        self.page_type = page_type
        self.is_dirty = is_dirty

        self.page_offset = 0
        if self.pgno == 1:
            self.page_offset = 100
        if self.page_type is None:
            if self.data[self.page_offset]:
                self.page_type = self.data[self.page_offset]
            else:
                self.page_type = BTREE_PAGE_TYPE_RAW_PAGE
        assert self.page_type is not None

    def _dump(self):
        print("  pgno=", self.pgno)
        for i in range(0, len(self.data), 32):
            print(
                "  {:04x}  {} {}".format(
                    i,
                    binascii.hexlify(self.data[i:i+16]).decode('ascii'),
                    binascii.hexlify(self.data[i+16:i+32]).decode('ascii'),
                )
            )

    @property
    def is_dirty(self):
        return self._is_dirty

    @is_dirty.setter
    def is_dirty(self, v):
        self._is_dirty = v
        if self._is_dirty:
            self.pager._set_page(self)

    def write(self, data, offset):
        "Write data"
        self.data[offset:offset + len(data)] = data
        self.is_dirty = True

    def initialize_page(self, page_type):
        "Initialize page as page_type page"
        self.page_type = page_type
        self.data[self.page_offset:self.pager.page_size-self.page_offset] = b'\x00' * (self.pager.page_size-self.page_offset)
        if page_type in (
            BTREE_PAGE_TYPE_LEAF_TABLE,
            BTREE_PAGE_TYPE_INTERIOR_TABLE,
            BTREE_PAGE_TYPE_LEAF_INDEX,
            BTREE_PAGE_TYPE_INTERIOR_INDEX,
        ):
            self.data[self.page_offset] = page_type
        self.is_dirty = True

    def get_node(self):
        "Get page btree node instance"
        return {
            BTREE_PAGE_TYPE_LEAF_TABLE: TableLeafNode,
            BTREE_PAGE_TYPE_INTERIOR_TABLE: TableInteriorNode,
            BTREE_PAGE_TYPE_LEAF_INDEX: IndexLeafNode,
            BTREE_PAGE_TYPE_INTERIOR_INDEX: IndexInteriorNode,
            BTREE_PAGE_TYPE_FREE_PAGE: FreePage,
            BTREE_PAGE_TYPE_RAW_PAGE: RawPage,
        }[self.page_type](self)

    def __str__(self):
        return "page{}".format(self.pgno)


class Pager:
    def __init__(self, database):
        self.database = database
        self.pages = {}

        self.database.fileobj.seek(0, 0)
        magic = self.database.fileobj.read(16)
        if magic != b"SQLite format 3\x00":
            self.database.fileobj.close()
            raise ValueError("Invalid Magic header")
        self.page_size = int.from_bytes(self.database.fileobj.read(2), 'big')
        if self.page_size == 1:
            self.page_size = 65536
        self.database.fileobj.seek(0, 2)
        file_size = self.database.fileobj.tell()
        if file_size % self.page_size != 0:
            raise ValueError("Invalid File size: {}".format(file_size))
        self.max_pgno = file_size // self.page_size

    def _dump(self):
        print("  page_size=", self.page_size)
        print("  file_change_counter=", self.file_change_counter)
        print("  header_btree_count=", self.header_btree_count)
        print("  pgno_first_freelist_trunk=", self.pgno_first_freelist_trunk)
        print("  num_freelist_pages=", self.num_freelist_pages)
        print("  max_pgno=", self.max_pgno)

    def _read_header(self, offset):
        page = self.get_page(1)
        return int.from_bytes(page.data[offset:offset+4], 'big')

    def _write_header(self, v, offset):
        page = self.get_page(1)
        for i, c in enumerate(v.to_bytes(4, "big")):
            page.data[offset+i] = c
        page.is_dirty = True

    def _set_page(self, page):
        self.pages[page.pgno] = page

    def _remove_page(self, pgno):
        self.pages[pgno] = None

    def find_rowid_table_path(self, pgno, rowid):
        """find ancestors TableInteriorNode list, TableLeafNode and cell index in that TableLeafNode
        return (list_of_interior_nodes, table_leaf_node, cell_index, found_or_not)
        matched record is one at most.
        """
        return self.get_page(pgno).get_node().find_rowid_table_path(rowid, [])

    def find_rowid_index_path(self, pgno, key, rowid, orders, recurse_to_leaf):
        """find ancestors IndexInteriorNode list, IndexLeafNode and cell index in that IndexLeafNode
        return (list_of_interior_nodes, index_leaf_node, cell_index, found_or_not)
        if found_or_not is True (=find record) return first record.
        """
        return self.get_page(pgno).get_node().find_rowid_index_path(key, rowid, orders, [], recurse_to_leaf)

    def rowid_range_records(self, pgno, min_rowid, max_rowid, converter=lambda rowid, record: (rowid, record)):
        "fetch table records by rowid range"
        return self.get_page(pgno).get_node().rowid_range_records(min_rowid, max_rowid, converter)

    def index_range_records(
        self, pgno, min_key, max_key, orders, positions, converter=lambda rowid, record: (rowid, record)
    ):
        "fetch table records by index range"
        node = self.get_page(pgno).get_node()
        return node.index_range_records(min_key, max_key, orders, positions, converter)

    def records(self, pgno, converter=lambda rowid, record: (rowid, record)):
        "fetch pgno table/index tree all records"
        return self.get_page(pgno).get_node().records(converter)

    # header variables
    @property
    def file_change_counter(self):
        return self._read_header(24)

    @file_change_counter.setter
    def filechange_counter(self, v):
        self._write_header(v, 24)

    @property
    def header_btree_count(self):
        return self._read_header(28)

    @header_btree_count.setter
    def header_btree_count(self, v):
        self._write_header(v, 28)

    @property
    def pgno_first_freelist_trunk(self):
        return self._read_header(32)

    @pgno_first_freelist_trunk.setter
    def pgno_first_freelist_trunk(self, v):
        self._write_header(v, 32)

    @property
    def num_freelist_pages(self):
        return self._read_header(36)

    @num_freelist_pages.setter
    def num_freelist_pages(self, v):
        self._write_header(v, 36)

    # end of header variables

    def close(self, ):
        self.database.fileobj.close()
        self.database.fileobj = None

    def get_page(self, pgno, page_type=None):
        "get pgno page"
        if pgno <= self.max_pgno:
            if not (page := self.pages.get(pgno)):
                # read page block
                self.database.fileobj.seek((pgno - 1) * self.page_size, 0)
                page = Page(self, pgno, self.database.fileobj.read(self.page_size), page_type, False)
            return page
        return None

    def move_page(self, from_pgno, to_pgno):
        page = self.get_page(from_pgno)
        self._remove_page(from_pgno)
        page.pgno = to_pgno
        page.is_dirty = True

    def _first_freelist_trunk(self):
        if self.pgno_first_freelist_trunk == 0:
            return None
        return self.get_page(self.pgno_first_freelist_trunk, page_type=BTREE_PAGE_TYPE_FREE_PAGE).get_node()

    def new_page(self, page_type=BTREE_PAGE_TYPE_FREE_PAGE):
        "get page from freelist or allocate new page and return new page"
        freelist_trunk = self._first_freelist_trunk()
        if freelist_trunk:
            page = freelist_trunk.pop_free_page()
        else:
            self.max_pgno += 1
            pgno = self.max_pgno
            page = Page(self, pgno, b'\x00' * self.page_size, BTREE_PAGE_TYPE_FREE_PAGE, True)
        page.initialize_page(page_type)
        return page

    def add_to_freelist(self, page):
        "add page to free list"
        page.initialize_page(BTREE_PAGE_TYPE_FREE_PAGE)
        freelist_trunk = self._first_freelist_trunk()
        if not freelist_trunk:
            self.pgno_first_freelist_trunk = page.pgno
        else:
            freelist_trunk.append_free_page(page)

    def rollback(self):
        self.pages = {}
        self.database.fileobj.seek(0, 2)
        self.max_pgno = self.database.fileobj.tell() // self.page_size

    def flush(self):
        "flush dirty pages"
        for pgno, page in self.pages.items():
            if page.is_dirty:
                self.database.fileobj.seek((pgno-1) * self.page_size, 0)
                self.database.fileobj.write(page.data)
        self.database.fileobj.flush()
        self.pages.clear()

    def __exit__(self, exc, value, traceback):
        self.close()
