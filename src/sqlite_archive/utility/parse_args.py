import argparse
import pathlib
def parse_args() -> argparse.Namespace:

    files_args: Tuple = ("files", "*")
    lowercase_table_args: Dict = {
        "long": "--lowercase-table",
        "action": "store_true",
        "dest": "lower",
        "help": "Modify the inferred table name to be lowercase (has no effect if table name is specified)."
    }
    table_arguments: Dict = {
        "long": "--table",
        "short": "-t",
        "dest": "table",
        "help": "Name of table to use."
    }
    autovacuum_args: Dict = {
        "long": "--autovacuum-mode",
        "short": "-a",
        "nargs": 1,
        "dest": "autovacuum",
        "default": 1,
        "type": int,
        "help": "Sets the automatic vacuum mode. (0 = disabled, 1 = full autovacuum mode, 2 = incremental autovacuum mode)",
        "choices": [0, 1, 2]
    }

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Imports or Exports files from an sqlite3 database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("db", type=str, help="SQLite DB filename.")
    parser.add_argument(
        "--debug",
        dest="debug",
        action="store_true",
        help="Supress any exception skipping and print some additional info.")
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print some more information without changing the exception raising policy."
    )
    parser.add_argument(autovacuum_args["long"],
                        autovacuum_args["short"],
                        nargs=autovacuum_args["nargs"],
                        default=autovacuum_args["default"],
                        dest=autovacuum_args["dest"],
                        choices=autovacuum_args["choices"],
                        help=autovacuum_args["help"])
    walargs = parser.add_mutually_exclusive_group()
    walargs.add_argument(
        "--wal",
        "-w",
        action="store_true",
        dest="wal",
        help="Use Write-Ahead Logging instead of rollback journal.")
    walargs.add_argument(
        "--rollback",
        "-r",
        action="store_true",
        dest="rollback",
        help="Switch back to rollback journal if Write-Ahead Logging is active."
    )

    subparsers: argparse._SubParsersAction = parser.add_subparsers(dest="mode")

    drop: argparse.ArgumentParser = subparsers.add_parser(
        'drop',
        aliases=['drop-table', 'drop_table'],
        help="Drop the specified table. NOTE: this will run VACUUM when done, by default.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    drop.add_argument("--no-vacuum",
                      dest="no_drop_vacuum",
                      action="store_true",
                      help="Do not execute VACUUM when dropping a table")
    drop.add_argument("table", help="Name of table to use")

    add = subparsers.add_parser(
        "add",
        help="Add files to the database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    add.add_argument(table_arguments["long"],
                     table_arguments["short"],
                     dest=table_arguments["dest"],
                     type=str,
                     help=table_arguments["help"])
    add.add_argument(
        "--replace",
        "-r",
        action="store_true",
        help="Replace any existing file entry's data instead of skipping. By default, the VACUUM command will be run to prevent database fragmentation."
    )
    add.add_argument(
        "--no-replace-vacuum",
        action="store_true",
        dest="no_replace_vacuum",
        help="Do not run the VACUUM command after replacing data.")
    add.add_argument(
        "--dups-file",
        type=str,
        dest="dups_file",
        help="Location of the file to store the list of duplicate files to.",
        default=f"{pathlib.Path.cwd().joinpath('duplicates.json')}")
    add.add_argument(
        "--no-dups",
        action="store_true",
        dest="nodups",
        help="Disables saving the duplicate list as a json file or reading an existing one from an existing file."
    )
    add.add_argument("--hide-dups",
                     dest="hidedups",
                     action="store_true",
                     help="Hides the list of duplicate files.")
    add.add_argument(
        "--dups-current-db",
        dest="dupscurrent",
        action="store_true",
        help="Only show the duplicates from the current database.")
    add.add_argument(lowercase_table_args["long"],
                     action=lowercase_table_args["action"],
                     dest=lowercase_table_args["dest"],
                     help=lowercase_table_args["help"])
    add.add_argument(
        "--no-atomic",
        action="store_true",
        dest="no_atomic",
        help="Run commit on every insert instead of at the end of the loop.")
    add.add_argument(
        "--exclude",
        action="append",
        dest="exclude",
        type=str,
        help="Name of a file to exclude from the file list (can be specified multiple times). Any directories specified are ignored at this time (per directory exclusion is WIP)."
    )
    add.add_argument("--vacuum",
                     action="store_true",
                     dest="vacuum",
                     help="Run VACUUM at the end.")
    add.add_argument("--convert-webp",
                     action="store_true",
                     dest="webp",
                     help="Convert any image files to webp")
    add.add_argument("--webp-compression-level",
                     dest="compression_level",
                     default=80,
                     help="Compression level for WebP conversion, (0-100, lower number means smaller files, but lower quality and vice versa.")
    add.add_argument("--webp-lossless",
                     dest="lossless",
                     action="store_true",
                     help="Convert image to a lossless webp image.")
    add.add_argument(files_args[0],
                     nargs=files_args[1],
                     type=str,
                     help="Files to be archived in the SQLite Database.")

    compact = subparsers.add_parser(
        "compact",
        help="Run the VACUUM query on the database (WARNING: depending on the size of the DB, it might take a while)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    create = subparsers.add_parser(
        "create",
        aliases=['create-table', 'create_table'],
        help="Runs the table creation queries and exits.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    create.add_argument("table", help=table_arguments["help"])

    extract: argparse.ArgumentParser = subparsers.add_parser(
        'extract',
        help="Extract files from a table instead of adding them.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    extract.add_argument(table_arguments["long"],
                         table_arguments["short"],
                         dest=table_arguments["dest"],
                         type=str,
                         help=table_arguments["help"])
    extract.add_argument(
        "--output-dir",
        "-o",
        dest="out",
        type=str,
        help="Directory to output files to. Defaults to a directory named after the table in the current directory. WARNING: Any existing files will have their data overwritten."
    )
    extract.add_argument(lowercase_table_args["long"],
                         action=lowercase_table_args["action"],
                         dest=lowercase_table_args["dest"],
                         help=lowercase_table_args["help"])
    extract.add_argument(
        "--force",
        "-f",
        dest="force",
        action="store_true",
        help="Forces extraction of a file from the database, even if the digest of the data does not match the one recorded in the database."
    )
    extract.add_argument(
        "--infer-pop-file",
        action="store_true",
        dest="pop",
        help="Removes the first entry in the file list when inferring the table name (has no effect when table name is specified)."
    )
    extract.add_argument(
        files_args[0],
        nargs=files_args[1],
        type=str,
        help="Files to be extracted from the SQLite Database. Leaving this empty will extract all files from the specified table."
    )
    upgrade: argparse.ArgumentParser = subparsers.add_parser(
        'upgrade',
        help="This is a placeholder argument but will be used if there are any schema upgrades.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    return parser.parse_args()