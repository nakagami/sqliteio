################################################################################
# MIT License
#
# Copyright (c) 2023, 2024 Hajime Nakagami<nakagami@gmail.com>
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
import binascii


__all__ = ("Page", "Pager", "FreePage")


class Page:
    __slots__ = ("pager", "pgno", "data", "_is_dirty", "page_offset")

    def __init__(self, pager, pgno, data):
        self.pager = pager
        self.pgno = pgno
        self.data = bytearray(data)
        self.is_dirty = False

        self.page_offset = 0
        if self.pgno == 1:
            self.page_offset = 100

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
            self.pager.set_page(self)

    def write(self, data, offset):
        "Write data"
        self.data[offset:offset + len(data)] = data
        self.is_dirty = True

    def initialize(self):
        "Initialize page as an empty page"
        self.data[self.page_offset:] = b'\x00' * (len(self.data) - self.page_offset)
        self.is_dirty = True

    def __str__(self):
        return "page{}".format(self.pgno)


class FreePage:
    "freelist trunk page"
    __slots__ = ("page",)

    def __init__(self, page):
        self.page = page

    def __eq__(self, other):
        return other and self.page.data[:8+(self.num_children*4)] == other.page.data[:8+(self.num_children*4)]

    def _dump(self):
        print("FreePage")
        print("\tnext_page={},{}".format(self.next_trunk_pgno, self.child_pgno_list()))

    @property
    def next_trunk_pgno(self):
        return int.from_bytes(self.page.data[:4], 'big')

    @next_trunk_pgno.setter
    def next_trunk_pgno(self, v):
        self.page.write(v.to_bytes(4, "big"), 0)

    @property
    def num_children(self):
        return int.from_bytes(self.page.data[4:8], 'big')

    @num_children.setter
    def num_children(self, v):
        self.page.write(v.to_bytes(4, "big"), 4)

    def child_pgno_list(self):
        # free page number list
        pgno_list = []
        for i in range(self.num_children):
            pgno = int.from_bytes(self.page.data[8 + i * 4:12 + i * 4], 'big')
            pgno_list.append(pgno)
        return pgno_list

    def get_next_trunk(self):
        if self.next_trunk_pgno == 0:
            return None
        return FreePage(self.page.pager.get_page(self.next_trunk_pgno))

    def append_free_page(self, free_page):
        trunk = self
        while self.page.pager.page_size == 8 + trunk.num_children * 4:
            next_trunk = trunk.get_next_trunk()
            if next_trunk is None:
                # use free_page as a new trunk page
                trunk.next_trunk_pgno = free_page.pgno
                return
            trunk = next_trunk
        trunk.page.write(free_page.pgno.to_bytes(4, "big"), 8 + trunk.num_children * 4)
        trunk.num_children += 1

    def pop_free_page(self):
        if self.num_children:
            self.num_children -= 1
            pgno = int.from_bytes(
                self.page.data[8 + self.num_children * 4:12 + self.num_children * 4], 'big'
            )
            self.page.write(b'\x00' * 4, 8 + self.num_children * 4)
            free_page = self.page.pager.get_page(pgno)
        else:
            self.page.pager.pgno_first_freelist_trunk = self.next_trunk_pgno
            free_page = self.page
        return free_page


class Pager:
    __slots__ = ("database", "pages", "page_size", "max_pgno")

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

    def set_page(self, page):
        self.pages[page.pgno] = page

    def remove_page(self, pgno):
        self.pages[pgno] = None

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

    def get_page(self, pgno):
        "get pgno page"
        if pgno <= self.max_pgno:
            if not (page := self.pages.get(pgno)):
                # read page block
                self.database.fileobj.seek((pgno - 1) * self.page_size, 0)
                page = Page(self, pgno, self.database.fileobj.read(self.page_size))
            return page
        return None

    def move_page(self, from_pgno, to_pgno):
        page = self.get_page(from_pgno)
        self.remove_page(from_pgno)
        page.pgno = to_pgno
        page.is_dirty = True

    def _first_freelist_trunk(self):
        if self.pgno_first_freelist_trunk == 0:
            return None
        return FreePage(self.get_page(self.pgno_first_freelist_trunk))

    def new_page(self):
        "get page from freelist or allocate new page and return new empty page"
        freelist_trunk = self._first_freelist_trunk()
        if freelist_trunk:
            page = freelist_trunk.pop_free_page()
        else:
            self.max_pgno += 1
            page = Page(self, self.max_pgno, b'\x00' * self.page_size)
        page.initialize()
        return page

    def add_to_freelist(self, page):
        "add page to free list"
        page.initialize()
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
                page.is_dirty = False
        self.database.fileobj.flush()
        self.pages.clear()

    def __exit__(self, exc, value, traceback):
        self.close()
