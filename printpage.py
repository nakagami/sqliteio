#!/usr/bin/env python3
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
"""print SQLite3 file page
"""
import sys
import sqliteio


def print_page(path):
    database = sqliteio.open(path)
    pager = database.pager

    free_pages = []
    freelist_trunk = pager._first_freelist_trunk()
    while freelist_trunk:
        pager._set_page(freelist_trunk.page)
        free_pages.extend(freelist_trunk.child_pgno_list())
        freelist_trunk = freelist_trunk.get_next_trunk()

    print(path)
    pager._dump()
    for pgno in range(1, pager.max_pgno + 1):
        if pgno in free_pages:
            continue

        page = pager.get_page(pgno)
        node = page.get_node()
        if node:
            node._dump()
        page._dump()

    database.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("printpage.py [path_to_database_file]", file=sys.stderr)
        sys.exit(0)
    print_page(sys.argv[1])
