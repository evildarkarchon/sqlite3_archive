from . import sqlite_archive
def main():
    sqlitearchive: SQLiteArchive = sqlite_archive.SQLiteArchive()

    if sqlitearchive.args.mode == 'create':
        sqlitearchive.schema()
    elif sqlitearchive.args.mode == 'drop':
        sqlitearchive.drop()
    elif sqlitearchive.args.mode == 'compact':
        sqlitearchive.compact()
    elif sqlitearchive.args.mode == 'extract':
        sqlitearchive.extract()
    elif sqlitearchive.args.mode == 'add':
        sqlitearchive.add()
    elif sqlitearchive.args.mode == 'upgrade':
        sqlitearchive.upgrade()

if __name__ == "__main__":
    main()