#!/usr/bin/env python3
import sys
import argparse
import sqlite3
import pathlib
import glob
import atexit
import hashlib
import json

from typing import Any

parser: argparse.ArgumentParser = argparse.ArgumentParser(description="Imports or Exports files from an sqlite3 database.")
parser.add_argument("--db", "-d", dest="db", type=str, required=True, help="SQLite DB filename.")
parser.add_argument("--table", "-t", dest="table", type=str, help="Name of table to use.")
parser.add_argument("--extract", "-x", dest="extract", action="store_true", help="Extract files from a table instead of adding them.")
parser.add_argument("--output-dir", "-o", dest="out", type=str, help="Directory to output files to, if in extraction mode (defaults to current directory).", default=str(pathlib.Path.cwd()))
parser.add_argument("--replace", "-r", action="store_true", help="Replace any existing file entry's data instead of skipping.")
parser.add_argument("--debug", dest="debug", action="store_true", help="Supress any exception skipping and some debug info.")
parser.add_argument("--dups-file", type=str, dest="dups", help="Location of the file to store the list of duplicate files to. Defaults to duplicates.json in current directory.", default="{}/duplicates.json".format(pathlib.Path.cwd()))
parser.add_argument("--no-dups", action="store_true", dest="nodups", help="Disables saving the duplicate list as a json file or reading an existing one from an existing file.")
parser.add_argument("--hide-dups", dest="hidedups", action="store_true", help="Hides the list of duplicate files.")
parser.add_argument("--full-dup-path", dest="fulldups", action="store_true", help="Use the full path of the duplicate file as the key for the duplicates list.")
parser.add_argument("--dups-current-db", dest="dupscurrent", action="store_true", help="Only show the duplicates from the current database.")
parser.add_argument("--compact", action="store_true", help="Run the VACUUM query on the database (WARNING: depending on the size of the DB, it might take a while, use sparingly)")
parser.add_argument("files", nargs="*", help="Files to be archived in the SQLite Database.")

args: argparse.Namespace = parser.parse_args()

