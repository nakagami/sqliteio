import binascii
from .record import varint_and_next_index, to_varint, decode_payload, pack_value_list

BTREE_PAGE_TYPE_RAW_PAGE = -1       # pseudo number
BTREE_PAGE_TYPE_FREE_PAGE = 0       # pseudo number
BTREE_PAGE_TYPE_INTERIOR_INDEX = 2
BTREE_PAGE_TYPE_INTERIOR_TABLE = 5
BTREE_PAGE_TYPE_LEAF_INDEX = 10
BTREE_PAGE_TYPE_LEAF_TABLE = 13


__all__ = (
    "swap_node",
    "TableLeafCell",
    "TableInteriorCell",
    "IndexLeafCell",
    "IndexInteriorCell",
    "TableLeafNode",
    "IndexLeafNode",
    "IndexInteriorNode",
    "FreePage",
    "RawPage",
)


def swap_node(node1, node2):
    "swap node page"
    page1 = node1.pager.get_page(node1.pgno)
    page2 = node2.pager.get_page(node2.pgno)
    page1.data, page2.data = page2.data, page1.data
    page1.page_type, page2.page_type = page2.page_type, page1.page_type
    return page1.get_node(), page2.get_node()


class CellPayload:
    def __init__(self, node, cell_pointer, payload_len, cell_content):
        assert payload_len >= 0
        in_page_bytes = node.calculate_cell_in_page_bytes(payload_len)
        self.cell_size = in_page_bytes
        if in_page_bytes == payload_len:
            first_payload = cell_content
            overflow_pgno = 0
        else:
            assert len(cell_content) >= in_page_bytes+4
            first_payload = cell_content[:in_page_bytes]
            overflow_pgno = int.from_bytes(cell_content[in_page_bytes:in_page_bytes+4], 'big')
            self.cell_size += 4
        self.node = node
        self.cell_pointer = cell_pointer
        self.payload_len = payload_len
        self.first_payload = first_payload
        self.overflow_pgno = overflow_pgno

    def get_payload_with_overflow(self):
        "get payload bytes with overflow"
        buf = self.first_payload[:]
        overflow = self.overflow_pgno
        while overflow:
            page = self.node.pager.get_page(overflow)
            overflow = int.from_bytes(page.data[:4], 'big')
            buf += page.data[4:]

        return buf[:self.payload_len]

    def free_overflow_pages(self):
        overflow_pgno = self.overflow_pgno
        while overflow_pgno:
            page = self.node.pager.get_page(overflow_pgno)
            overflow_pgno = int.from_bytes(page.data[:4], 'big')
            self.node.pager.add_to_freelist(page)

    def __repr__(self):
        return binascii.hexlify(self.first_payload).decode('utf-8')


class Cell:
    @property
    def cell_block(self):
        "buffer data pointed by cell pointer"
        return self.node.page.data[self.cell_pointer:self.cell_pointer+self.size]


class TableLeafCell(Cell):
    def __init__(self, node, cell_pointer):
        self.node = node
        self.cell_pointer = cell_pointer
        payload_len, next_i = varint_and_next_index(node.page.data, cell_pointer)
        self.rowid, next_i = varint_and_next_index(node.page.data, next_i)
        self.cell_payload = CellPayload(node, cell_pointer, payload_len, node.page.data[next_i:])
        self.size = (next_i - cell_pointer) + self.cell_payload.cell_size

    def _dump(self):
        print(binascii.hexlify(self.cell_block).decode('utf-8'))
        print(decode_payload(self.cell_payload.get_payload_with_overflow()))


class TableInteriorCell(Cell):
    def __init__(self, node, cell_pointer):
        self.node = node
        self.cell_pointer = cell_pointer
        key, next_i = varint_and_next_index(node.page.data, cell_pointer + 4)
        self.key = key
        self.size = next_i - cell_pointer

    @property
    def left_page(self):
        return int.from_bytes(self.node.page.data[self.cell_pointer:self.cell_pointer + 4], 'big')

    @left_page.setter
    def left_page(self, new_pgno):
        self.left_pgno = new_pgno
        self.node._write_page(self.left_pgno.to_bytes(4, "big"), self.cell_pointer)


class IndexLeafCell(Cell):
    def __init__(self, node, cell_pointer):
        self.node = node
        self.cell_pointer = cell_pointer
        payload_len, next_i = varint_and_next_index(node.page.data, cell_pointer)
        self.cell_payload = CellPayload(node, cell_pointer, payload_len, node.page.data[next_i:])
        self.size = (next_i - cell_pointer) + self.cell_payload.cell_size


class IndexInteriorCell(Cell):
    def __init__(self, node, cell_pointer):
        self.node = node
        self.cell_pointer = cell_pointer
        self.left_page = int.from_bytes(node.page.data[cell_pointer:cell_pointer + 4], 'big')
        payload_len, next_i = varint_and_next_index(node.page.data, cell_pointer + 4)
        self.cell_payload = CellPayload(node, cell_pointer, payload_len, node.page.data[next_i:])
        self.size = (next_i - cell_pointer) + self.cell_payload.cell_size


class BTreeNode:
    def __init__(self, page):
        self.pgno = page.pgno
        self.pager = page.pager
        self.page_offset = 0
        if page.pgno == 1:
            self.page_offset = 100

    def __str__(self):
        return "{}({})".format(self.__class__.__name__, self.pgno)

    def __eq__(self, other):
        return other and self.page.data == other.page.data

    def _dump(self):
        print("  pgno=", self.page.pgno)
        print("  btree_page_type=", {
            TableLeafNode: "LeafTable",
            TableInteriorNode: "InteriorTable",
            IndexLeafNode: "LeafIndex",
            IndexInteriorNode: "InteriorIndex",
            FreePage: "FreePage",
            RawPage: "RawPage",
        }[type(self)])
        print("  free_block_offset=", hex(self.free_block_offset))
        print("  number_of_cells=", self.number_of_cells)
        print("  first_byte_of_cell_content=", hex(self.first_byte_of_cell_content))
        print("  number_of_fragmented_free_bytes=", self.number_of_fragmented_free_bytes)
        if self.page.page_type in (BTREE_PAGE_TYPE_INTERIOR_INDEX, BTREE_PAGE_TYPE_INTERIOR_TABLE):
            print("  right_most=", self.right_most)
        print("  cell_pointers=", [hex(i) for i in self.read_cell_pointers()])
        if self.free_block_offset:
            print("  free_block")
            free_block_offset = self.free_block_offset
            while free_block_offset:
                next_offset, size = self._next_free_block_offset_and_current_free_block_size(free_block_offset)
                print("    {}:{}->{}".format(hex(free_block_offset), size, hex(next_offset)))
                free_block_offset = next_offset

    def _cmp_key(self, key, record, positions, orders):
        """compare key and record
        0: key equal record
        -1: less key than record
        1: bigger key than record
        """
        assert isinstance(self, (IndexInteriorNode, IndexLeafNode))
        for i in range(len(key)):
            c = (key[i] > record[positions[i]]) - (key[i] < record[positions[i]])
            if c != 0:
                return c * orders[i]
        return 0

    def _next_free_block_offset_and_current_free_block_size(self, cell_offset):
        next_free_block_offset = int.from_bytes(self.page.data[cell_offset:cell_offset+2], 'big')
        current_free_block_size = int.from_bytes(self.page.data[cell_offset+2:cell_offset+4], 'big')
        return next_free_block_offset, current_free_block_size

    def _merge_free_block(self, cell_offset):
        next_offset, size = self._next_free_block_offset_and_current_free_block_size(cell_offset)
        if cell_offset + size == next_offset:
            next_next_offset, next_size = self._next_free_block_offset_and_current_free_block_size(next_offset)
            self._write_page(next_next_offset.to_bytes(2, "big"), cell_offset)
            self._write_page((size+next_size).to_bytes(2, "big"), cell_offset+2)
            self._write_page(b'\x00\x00', next_offset)
            self._write_page(b'\x00\x00', next_offset+2)

            return True
        return False

    def _write_page(self, data, offset):
        self.pager.get_page(self.pgno).write(data, offset)

    @property
    def page(self):
        return self.pager.get_page(self.pgno)

    @property
    def free_block_offset(self):
        return int.from_bytes(self.page.data[self.page_offset+1:self.page_offset+3], 'big')

    @free_block_offset.setter
    def free_block_offset(self, v):
        self._write_page(v.to_bytes(2, "big"), self.page_offset + 1)

    @property
    def number_of_cells(self):
        return int.from_bytes(self.page.data[self.page_offset+3:self.page_offset+5], 'big')

    @number_of_cells.setter
    def number_of_cells(self, v):
        self._write_page(v.to_bytes(2, "big"), self.page_offset + 3)

    @property
    def first_byte_of_cell_content(self):
        return int.from_bytes(self.page.data[self.page_offset+5:self.page_offset+7], 'big')

    @first_byte_of_cell_content.setter
    def first_byte_of_cell_content(self, v):
        self._write_page(v.to_bytes(2, "big"), self.page_offset + 5)

    @property
    def number_of_fragmented_free_bytes(self):
        return int.from_bytes(self.page.data[self.page_offset+7:self.page_offset+8], 'big')

    @number_of_fragmented_free_bytes.setter
    def number_of_fragmented_free_bytes(self, v):
        self._write_page(bytes([v]), self.page_offset + 7)

    @property
    def right_most(self):
        assert self.page.page_type in (BTREE_PAGE_TYPE_INTERIOR_INDEX, BTREE_PAGE_TYPE_INTERIOR_TABLE)
        return int.from_bytes(self.page.data[self.page_offset+8:self.page_offset+12], 'big')

    @right_most.setter
    def right_most(self, v):
        assert self.page.page_type in (BTREE_PAGE_TYPE_INTERIOR_INDEX, BTREE_PAGE_TYPE_INTERIOR_TABLE)
        self._write_page(v.to_bytes(4, "big"), self.page_offset + 8)

    def _first_payload_and_trailing(self, r):
        if self.max_in_page_payload() >= len(r):
            return r
        i = self.calculate_cell_in_page_bytes(len(r))
        first_payload = r[:i]
        trailing_pages = []
        while r[i:]:
            j = i + self.pager.page_size -4
            trailing_pages.append(r[i:j])
            i = j

        assert len(first_payload) > 0
        assert len(trailing_pages) > 0

        next_page = self.pager.new_page(BTREE_PAGE_TYPE_RAW_PAGE)
        first_next_pgno = next_page.pgno
        p = trailing_pages.pop(0)
        next_page.data[4:len(p)+4] = p
        while trailing_pages:
            next_next_page = self.pager.new_page(BTREE_PAGE_TYPE_RAW_PAGE)
            next_page.data[0:4] = next_next_page.pgno.to_bytes(4, "big")
            p = trailing_pages.pop(0)
            next_next_page.data[4:len(p)+4] = p
            next_page = next_next_page

        return first_payload + first_next_pgno.to_bytes(4, "big")

    def sweep(self):
        "sweep cell blocks"
        cell_blocks = bytearray()
        cell_pointers = []
        first_byte_of_cell_content = self.page.pager.page_size
        for cell in self.cells:
            cell_blocks = cell.cell_block[:] + cell_blocks[:]
            first_byte_of_cell_content -= len(cell.cell_block)
            cell_pointers.append(first_byte_of_cell_content)

        self.free_block_offset = 0
        self.page.data[self.first_cell_offset:self.page.pager.page_size] = (
            b'\x00' * (self.page.pager.page_size - self.first_cell_offset)
        )
        self.page.data[first_byte_of_cell_content:self.page.pager.page_size] = cell_blocks
        self.write_cell_pointers(cell_pointers)

        self._update_cells()

    def find_cell_pointer_from_free_block(self, cell_block_size):
        """find cell_pointer from free_block
        Returns None if there is not enough free_block
        """
        if self.free_block_offset:
            free_block_offset = self.free_block_offset
            while free_block_offset:
                next_offset, size = self._next_free_block_offset_and_current_free_block_size(free_block_offset)
                if size == cell_block_size:
                    if self.free_block_offset == free_block_offset:
                        self.free_block_offset = next_offset
                        return free_block_offset
                free_block_offset = next_offset
        return None

    def merge_free_block(self):
        free_block_offset = self.free_block_offset
        while free_block_offset:
            if self._merge_free_block(free_block_offset):
                free_block_offset = self.free_block_offset
            else:
                next_offset, size = self._next_free_block_offset_and_current_free_block_size(free_block_offset)
                free_block_offset = next_offset

        # clear free_block_offset
        next_offset, size = self._next_free_block_offset_and_current_free_block_size(self.free_block_offset)
        if self.free_block_offset + size == self.first_byte_of_cell_content:
            self.free_block_offset = next_offset

    def read_cell_pointers(self):
        return [
            int.from_bytes(self.page.data[i:i+2], 'big')
            for i in range(self.first_cell_offset, self.first_cell_offset+self.number_of_cells*2, 2)
        ]

    def write_cell_pointers(self, pointers):
        for i, p in enumerate(pointers):
            self._write_page(p.to_bytes(2, "big"), self.first_cell_offset + i * 2)
        self.number_of_cells = len(pointers)
        self._write_page(self.number_of_cells.to_bytes(2, "big"), 3)

    def free_cell_size(self):
        cell_pointers = self.read_cell_pointers()
        if len(cell_pointers) == 0:
            min_cell_pointer = self.page.pager.page_size
        else:
            min_cell_pointer = min(cell_pointers)
        return min_cell_pointer - (self.first_cell_offset + (self.number_of_cells + 1) * 2)

    def get_free_cell_size(self):
        cell_pointers = self.read_cell_pointers()
        return min(cell_pointers) - (self.first_cell_offset + (self.number_of_cells + 1) * 2)

    def _update_cells(self):
        raise NotImplementedError()

    def insert_cell_block(self, cell_index, cell_block):
        assert self.free_cell_size() >= len(cell_block)

        cell_pointers = self.read_cell_pointers()
        if len(cell_pointers) == 0:
            cell_pointer = self.page.pager.page_size - len(cell_block)
        else:
            cell_pointer = self.find_cell_pointer_from_free_block(len(cell_block))
            if cell_pointer is None:
                cell_pointer = min(cell_pointers) - len(cell_block)
        self._write_page(cell_block, cell_pointer)
        cell_pointers = self.read_cell_pointers()
        cell_pointers.insert(cell_index, cell_pointer)
        self.write_cell_pointers(cell_pointers)
        self.first_byte_of_cell_content = min(cell_pointers)
        self._update_cells()

    def append_cell_block(self, cell_block):
        return self.insert_cell_block(self.number_of_cells, cell_block)

    def calculate_cell_in_page_bytes(self, payload_len):
        u = self.pager.page_size
        p = payload_len
        x = self.max_in_page_payload()
        m = ((u - 12) * 32 // 255) - 23
        k = m + ((p - m) % (u - 4))

        if p <= x:
            return payload_len
        elif k <= x:
            return k
        else:
            return m

    def delete(self, cell_index):
        cell = self.cells.pop(cell_index)

        # remove overflow pages
        cell.cell_payload.free_overflow_pages()
        # fill cell with zeros
        self._write_page(b'\x00' * cell.size, cell.cell_pointer)
        # free block size
        self._write_page(cell.size.to_bytes(2, "big"), cell.cell_pointer+2)

        # recalc free_block_offset and first_byte_of_cell_content
        if len(self.cells) == 0:
            self.free_block_offset = 0
            self.first_byte_of_cell_content = self.page.pager.page_size
        else:
            if self.free_block_offset == 0:
                self.free_block_offset = cell.cell_pointer
            else:
                self._write_page(self.free_block_offset.to_bytes(2, "big"), cell.cell_pointer)
                self.free_block_offset = cell.cell_pointer
            self.first_byte_of_cell_content = min([c.cell_pointer for c in self.cells])

        # save remaining pointers
        self.write_cell_pointers([c.cell_pointer for c in self.cells])

        self.merge_free_block()

    def append_cell_from(self, from_node, cell):
        assert type(self) == type(from_node)
        self.append_cell_block(cell.cell_block)


class LeafNodeMixIn:
    @property
    def first_cell_offset(self):
        return self.page_offset + 8


class InteriorNodeMixIn:
    @property
    def first_cell_offset(self):
        return self.page_offset + 12


class TableLeafNode(BTreeNode, LeafNodeMixIn):
    def _update_cells(self):
        self.cells = [TableLeafCell(self, c) for c in self.read_cell_pointers()]

    def __init__(self, page):
        super().__init__(page)
        self._update_cells()

    def __eq__(self, other):
        if self.page.data[:8] != other.page.data[:8]:
            return False

        assert len(self.cells) == self.number_of_cells
        assert len(other.cells) == self.number_of_cells

        for i in range(self.number_of_cells):
            c1 = self.cells[i]
            c2 = other.cells[i]
            if c1.cell_block != c2.cell_block:
                return False
        return True

    def _dump(self):
        print("TableLeafNode")
        super()._dump()
        print("  records")
        for cell in self.cells:
            print("    rowid={},size={},payload.cell_size={},payload_len={}{}".format(
                    cell.rowid,
                    cell.size,
                    cell.cell_payload.cell_size,
                    cell.cell_payload.payload_len, decode_payload(cell.cell_payload.get_payload_with_overflow())
                )
            )

    def _pack_record(self, rowid, value_list):
        payload = pack_value_list(value_list)
        return to_varint(len(payload)), to_varint(rowid), payload

    def split_by_index(self, cell_index):
        assert self.number_of_cells >= cell_index

        # create new empty node
        new_page = self.page.pager.new_page()
        new_page.initialize_page(self.page.page_type)
        new_node = new_page.get_node()

        # copy right cells to new_node
        for i in range(cell_index, self.number_of_cells):
            new_node.append_cell_from(self, self.cells[i])

        # delete rite cells
        while len(self.cells) > cell_index:
            self.delete(len(self.cells) - 1)

        return new_node

    def to_cell_block(self, rowid, value_list):
        ln, rowid, payload = self._pack_record(rowid, value_list)
        r = ln + rowid + payload
        cell_in_page_bytes = self.calculate_cell_in_page_bytes(len(r))
        if cell_in_page_bytes == len(r):    # enough space
            return r

        payload = self._first_payload_and_trailing(payload)
        r = ln + rowid + payload
        return r

    def find_cell_index(self, rowid):
        cell_index = 0
        for i, cell in enumerate(self.cells):
            if cell.rowid >= rowid:
                break
            cell_index = i + 1
        return cell_index

    def insert(self, rowid, cell_index, cell_block):
        self.insert_cell_block(cell_index, cell_block)

    def max_in_page_payload(self):
        return self.pager.page_size - 35

    def find_rowid_table_path(self, rowid, ancestors):
        for i, cell in enumerate(self.cells):
            if rowid > cell.rowid:
                continue
            elif rowid > cell.rowid:
                break
            return ancestors, self, i, rowid == cell.rowid
        return ancestors, self, len(self.cells), False

    def rowid_range_records(self, min_rowid, max_rowid, converter):
        for cell in self.cells:
            if min_rowid > cell.rowid:
                continue
            elif max_rowid > cell.rowid:
                break
            yield converter(cell.rowid, decode_payload(cell.cell_payload.get_payload_with_overflow()))

    def records(self, converter):
        for cell in self.cells:
            yield converter(cell.rowid, decode_payload(cell.cell_payload.get_payload_with_overflow()))

    def record(self, cell_index, converter):
        cell = self.cells[cell_index]
        return converter(cell.rowid, decode_payload(cell.cell_payload.get_payload_with_overflow()))


class TableInteriorNode(BTreeNode, InteriorNodeMixIn):
    @classmethod
    def new_node(cls, pager):
        return pager.new_page(BTREE_PAGE_TYPE_INTERIOR_TABLE).get_node()

    def _update_cells(self):
        self.cells = [TableInteriorCell(self, c) for c in self.read_cell_pointers()]

    def __init__(self, page):
        super().__init__(page)
        self._update_cells()

    def _dump(self):
        print("TableInteriorNode")
        super()._dump()
        for cell in self.cells:
            print("\tleft={},key={},size={}".format(cell.left_page, cell.key, cell.size))

    def _pack_record(self, left_pgno, key):
        return left_pgno.to_bytes(4, "big") + to_varint(key)

    def max_in_page_payload(self):
        raise NotImplementedError()

    def split_by_index(self, cell_index):
        assert self.number_of_cells >= cell_index

        # create new empty node
        new_page = self.page.pager.new_page()
        new_page.initialize_page(self.page.page_type)
        new_node = new_page.get_node()

        # copy right cells to new_node
        for i in range(cell_index, self.number_of_cells):
            new_node.append_cell_from(self, self.cells[i])

        # delete rite cells
        while len(self.cells) > cell_index:
            self.delete(len(self.cells) - 1)

        new_node.right_most = self.right_most
        self.right_most = new_node.pgno
        self.sweep()

        return new_node

    def insert_node_index(self, leaf_node, cell_index):
        cell_block = self._pack_record(leaf_node.page.pgno, leaf_node.cells[-1].rowid)
        if self.free_cell_size() < len(cell_block):
            new_node = self.split_by_index(cell_index)
            if self.free_cell_size() < len(cell_block):
                new_node.append_cell_block(cell_block)
            else:
                self.append_cell_block(cell_block)
        else:
            self.insert_cell_block(cell_index, cell_block)

    def insert_node_after(self, leaf_node, ancestors, prev_leaf_node):
        """insert TaleLeafNode page leaf_node befre prev_leaf_node
        """
        if self.right_most == prev_leaf_node.page.pgno:
            self.right_most = leaf_node.page.pgno
            self.insert_node_index(prev_leaf_node, len(self.cells))
        else:
            raise ValueError("TODO:")

    def find_rowid_table_path(self, rowid, ancestors):
        for cell in self.cells:
            if rowid > cell.key:
                continue
            if rowid > cell.key:
                break
            ancestors.append(self)
            node = self.page.pager.get_page(cell.left_page).get_node()
            return node.find_rowid_table_path(rowid, ancestors)
        if rowid > self.cells[-1].key:
            ancestors.append(self)
            node = self.page.pager.get_page(self.right_most).get_node()
            return node.find_rowid_table_path(rowid, ancestors)
        raise ValueError("Unexpected node:{}".format(self.page.pgno))

    def rowid_range_records(self, min_rowid, max_rowid, converter):
        for cell in self.cells:
            if min_rowid > cell.key:
                continue
            if max_rowid > cell.key:
                break
            node = self.page.pager.get_page(cell.left_page).get_node()
            for r in node.rowid_range_records(min_rowid, max_rowid, converter):
                yield r
        if max_rowid > self.cells[-1].key:
            node = self.page.pager.get_page(self.right_most).get_node()
            for r in node.rowid_range_records(min_rowid, max_rowid, converter):
                yield r

    def records(self, converter):
        for cell in self.cells:
            node = self.page.pager.get_page(cell.left_page).get_node()
            for r in node.records(converter):
                yield r
        node = self.page.pager.get_page(self.right_most).get_node()
        for r in node.records(converter):
            yield r

    def merge_children(self):
        page = self.pager.get_page(self.pgno)
        children = [self.pager.get_page(c.left_page).get_node() for c in self.cells]
        if self.right_most:
            children.append(self.pager.get_page(self.right_most).get_node())
        amount_of_page_size = self.page_offset + 8
        for child in children:
            amount_of_page_size += (child.number_of_cells * 2) + sum([c.size for c in child.cells])
        if amount_of_page_size > self.page.pager.page_size:
            return

        # merge children and remove TableInteriorNode
        page.initialize_page(BTREE_PAGE_TYPE_LEAF_TABLE)
        table_leaf_node = page.get_node()
        for child in children:
            for cell in child.cells:
                table_leaf_node.append_cell_from(child, cell)
            self.pager.add_to_freelist(child.page)

    def delete(self, cell_index):
        raise NotImplementedError()


class IndexLeafNode(BTreeNode, LeafNodeMixIn):
    def _update_cells(self):
        self.cells = [self._parse_index_leaf(c) for c in self.read_cell_pointers()]
        self.cells = [IndexLeafCell(self, c) for c in self.read_cell_pointers()]

    def __init__(self, page):
        super().__init__(page)
        self._update_cells()

    def __eq__(self, other):
        if self.page.data[:8] != other.page.data[:8]:
            return False

        assert len(self.cells) == self.number_of_cells
        assert len(other.cells) == self.number_of_cells

        for i in range(self.number_of_cells):
            if self.cells[i].cell_block != other.cells[i].cell_block:
                return False
        return True

    def _dump(self):
        print("IndexLeafNode")
        super()._dump()
        print("  records")
        for cell in self.cells:
            print("    {}".format(decode_payload(cell.cell_payload.get_payload_with_overflow())))

    def _pack_record(self, key, rowid):
        r = pack_value_list(key + [rowid])
        return to_varint(len(r)) + r

    def split_by_median(self):
        assert self.number_of_cells >= 3
        cell_index = self.number_of_cells - self.number_of_cells // 2
        cell_block = self.cells[cell_index].cell_block

        # create new empty node
        new_page = self.page.pager.new_page()
        new_page.initialize_page(self.page.page_type)
        new_node = new_page.get_node()

        for i in range(cell_index+1, self.number_of_cells):
            new_node.append_cell_from(self, self.cells[i])
        while len(self.cells) > cell_index:
            self.delete(len(self.cells) - 1)

        return new_node, cell_block

    def to_cell_block(self, rowid, key):
        r = self._pack_record(key, rowid)
        cell_in_page_bytes = self.calculate_cell_in_page_bytes(len(r))
        if cell_in_page_bytes == len(r):    # enough space
            return r
        return self._first_payload_and_trailing(r)

    def find_cell_index(self, key, index_schema):
        cell_index = 0
        for i, cell in enumerate(self.cells):
            record = decode_payload(cell.cell_payload.get_payload_with_overflow())
            _cmp = self._cmp_key(key, record, range(len(key)), index_schema.orders)
            if _cmp < 0:
                break
            cell_index = i + 1
        return cell_index

    def insert(self, rowid, key, cell_index, cell_block):
        self.insert_cell_block(cell_index, cell_block)

    def _parse_index_leaf(self, cell_pointer):
        payload_len, next_i = varint_and_next_index(self.page.data, cell_pointer)
        return CellPayload(self, cell_pointer, payload_len, self.page.data[next_i:])

    def max_in_page_payload(self):
        return ((self.pager.page_size-12)*64//255)-2

    def find_rowid_index_path(self, key, rowid, orders, ancestors, recurse_to_leaf):
        for i, cell in enumerate(self.cells):
            record = decode_payload(cell.cell_payload.get_payload_with_overflow())
            _cmp = self._cmp_key(key, record, range(len(key)), orders)
            if _cmp > 0:
                continue
            elif _cmp < 0:
                return ancestors, self, i, False
            if record[-1] == rowid:
                return ancestors, self, i, True
            else:
                # key matched but rowid does not match
                pass

        return ancestors, self, len(self.cells), False

    def index_range_records(self, min_key, max_key, orders, positions, converter):
        for cell in self.cells:
            record = decode_payload(cell.cell_payload.get_payload_with_overflow())
            if self._cmp_key(min_key, record, positions, orders) > 0:
                continue
            elif self._cmp_key(max_key, record, positions, orders) < 0:
                break
            yield converter(None, record)

    def records(self, converter):
        for cell in self.cells:
            yield converter(None, decode_payload(cell.cell_payload.get_payload_with_overflow()))


class IndexInteriorNode(BTreeNode, InteriorNodeMixIn):
    @classmethod
    def new_node(cls, pager):
        return pager.new_page(BTREE_PAGE_TYPE_INTERIOR_INDEX).get_node()

    def _update_cells(self):
        self.cells = [IndexInteriorCell(self, c) for c in self.read_cell_pointers()]

    def __init__(self, page):
        super().__init__(page)
        self._update_cells()

    def _dump(self):
        print("IndexInteriorNode")
        super()._dump()
        for cell in self.cells:
            print("\t{}:{}".format(cell.left_page, decode_payload(cell.cell_payload.get_payload_with_overflow())))
        print("\tright_most={}".format(self.right_most))

    def _pack_record(self, left_pgno, value_list):
        packed = pack_value_list(value_list)
        return left_pgno.to_bytes(4, "big") + to_varint(len(packed)) + packed

    def split_by_median(self, ancestors):
        assert self.number_of_cells >= 3
        cell_index = self.number_of_cells - self.number_of_cells // 2
        left_page = self.cells[cell_index].left_page

        # create new empty node
        new_page = self.page.pager.new_page()
        new_page.initialize_page(self.page.page_type)
        new_node = new_page.get_node()

        for i in range(cell_index+1, self.number_of_cells):
            new_node.append_cell_from(self, self.cells[i])
        while len(self.cells) > cell_index:
            self.delete(len(self.cells) - 1)
        new_node.right_most = self.right_most
        self.right_most = left_page
        if len(ancestors) == 0:
            parent = IndexInteriorNode.new_node(self.page.pager)
            elder, parent = swap_node(parent, self)
            new_node, elder = swap_node(elder, new_node)
        else:
            # TODO:
            raise NotImplementedError()
        return

    def max_in_page_payload(self):
        return ((self.pager.page_size-12)*64//255)-2

    def find_rowid_index_path(self, key, rowid, orders, ancestors, recurse_to_leaf):
        for i, cell in enumerate(self.cells):
            record = decode_payload(cell.cell_payload.get_payload_with_overflow())

            cmp = self._cmp_key(key, record, range(len(key)), orders)
            if cmp == 0:
                if record[-1] == rowid:
                    ancestors.append(self)
                    return ancestors, None, i, True
                node = self.page.pager.get_page(cell.left_page).get_node()
                ancestors2, leaf2, idx2, found = node.find_rowid_index_path(
                    key, rowid, orders, ancestors + [self], recurse_to_leaf
                )
                if found:
                    return ancestors2, leaf2, idx2, found
            elif cmp > 0:
                continue
            elif cmp < 0:
                ancestors.append(self)
                if recurse_to_leaf:
                    node = self.page.pager.get_page(cell.left_page).get_node()
                    return node.find_rowid_index_path(key, rowid, orders, ancestors, recurse_to_leaf)
                else:
                    return ancestors, None, i, False
        ancestors.append(self)
        node = self.page.pager.get_page(self.right_most).get_node()
        return node.find_rowid_index_path(key, rowid, orders, ancestors, recurse_to_leaf)

    def index_range_records(self, min_key, max_key, orders, positions, converter):
        for cell in self.cells:
            record = decode_payload(cell.cell_payload.get_payload_with_overflow())
            min_cmp = self._cmp_key(min_key, record, positions, orders)
            max_cmp = self._cmp_key(max_key, record, positions, orders)
            if min_cmp == 0 or max_cmp == 0:
                yield converter(None, record)
            elif min_cmp > 0:
                continue
            elif max_cmp > 0:
                break
            node = self.page.pager.get_page(cell.left_page).get_node()
            for r in node.index_range_records(min_key, max_key, orders, positions, converter):
                yield r
        record = decode_payload(self.cells[-1].cell_payload.get_payload_with_overflow())
        if self._cmp_key(max_key, record, positions, orders) > 0:
            node = self.page.pager.get_page(self.right_most).get_node()
            for r in node.index_range_records(min_key, max_key, orders, positions, converter):
                yield r

    def records(self, converter):
        for cell in self.cells:
            node = self.page.pager.get_page(cell.left_page).get_node()
            for r in node.records(converter):
                yield r
        node = self.page.pager.get_page(self.right_most).get_node()
        for r in node.records(converter):
            yield r


class FreePage(BTreeNode):
    def __init__(self, page):
        super().__init__(page)

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
        self._write_page(v.to_bytes(4, "big"), 0)

    @property
    def num_children(self):
        return int.from_bytes(self.page.data[4:8], 'big')

    @num_children.setter
    def num_children(self, v):
        self._write_page(v.to_bytes(4, "big"), 4)

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
        page = self.page.pager.get_page(self.next_trunk_pgno, page_type=BTREE_PAGE_TYPE_FREE_PAGE)
        node = page.get_node()
        assert isinstance(node, FreePage)
        return node

    def append_free_page(self, free_page):
        page = self.pager.get_page(self.pgno)
        trunk = page.get_node()
        while self.pager.page_size == 8 + trunk.num_children * 4:
            if trunk.get_next_trunk() is None:
                # append new FreePage
                new_trunk = FreePage(free_page.page)
                trunk.next_trunk_pgno = new_trunk.page.pgno
                return
            trunk = trunk.get_next_trunk()
        self._write_page(free_page.pgno.to_bytes(4, "big"), 8 + trunk.num_children * 4)
        trunk.num_children += 1

    def pop_free_page(self):
        if self.num_children:
            self.num_children -= 1
            pgno = int.from_bytes(
                self.page.data[8 + self.num_children * 4:12 + self.num_children * 4]
            )
            self.page.data[8 + self.num_children * 4:12 + self.num_children * 4] = b'\x00' * 4
            free_page = self.page.pager.get_page(pgno)
        else:
            self.page.pager.pgno_first_freelist_trunk = self.next_trunk_pgno
            self.page.initialize_page(BTREE_PAGE_TYPE_FREE_PAGE)
            free_page = self.page
        return free_page


class RawPage(BTreeNode):
    def __init__(self, page):
        super().__init__(page)

    def _dump(self):
        print("RawPage")
