#!/usr/bin/env python3
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


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("ppage [path_to_database_file]")
        sys.exit(0)
    print_page(sys.argv[1])
