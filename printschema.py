#!/usr/bin/env python3
################################################################################
# MIT License
#
# Copyright (c) 2024 Hajime Nakagami<nakagami@gmail.com>
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
"""print SQLite3 schema
"""
import sys
import sqliteio


def print_schema(path):
    database = sqliteio.open(path)
    for t in database.tables.values():
        print(f"\n{' ' + t.name + ' ':-^80}")
        t._dump()
        if t.name in database.indexes:
            for idx in database.indexes[t.name]:
                idx._dump()

    database.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("printschema.py [path_to_database_file]", file=sys.stderr)
        sys.exit(0)
    print_schema(sys.argv[1])
