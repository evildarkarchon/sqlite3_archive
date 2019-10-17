# sqlite3_archive

Python version of my sqlite3_archive project. This script requires at least version 3.7 of python because it takes advantage of features added in that version. I only test this script on Windows since that's the operating system of the machine I write this script on. I try to keep Windows specific stuff out of the script and I use pathlib to deal with all of the path manipulation, so it should work just fine on both Unix/Linux and Windows platforms.

---

This script's purpose is to store files as BLOBs in an sqlite3 database, along with a couple pieces of metadata (file name and sha512 hash). More metadata will be added as needed. The schema is not frozen yet, so newer versions of this script may be incompatible with older databases. Luckily, there are plenty of tools (including the original sqlite3 command line program) that can read sqlite3 databases.

---

By default, this script will print a list of files that failed the UNIQUE constraint along with the file it conflicted with. It will also store said duplicates list in a json file named duplicates.json (File location is changeable on the command line). If you leave the ability to save the json file enabled and you keep using the same file name, you can keep track of duplicates from other directories as well. Obviously, this will only show duplicate files that were put in the same table, duplicate files may still exist elsewhere in the database if you use multiple tables (which you should). By default, the json keys will be relative to the parent directory of the file specified, but there is an option to have it put the full path of the conflicting file in the json key. Putting a full path to the original file is obviously not possible because that information is not stored in the database. Also, the list is stored in a key that consists of the database's path (relative or full).

---

There are at least 2 good options for running this in your PATH. One is the setuptools-based setup.py script which will bundle up the files and make a script to run it. The other is [PyInstaller](https://pypi.org/project/PyInstaller/) which will bundle all the files into a single executable with a standalone python installation that has only the components required to run the script. That mini-installation can be linked into the executable with the --onefile option or you can use the default, which will put the mini-python installation into a directory and put the scripts specified on the command-line into an executable along with it (you will have to specify all the files in the src directory (with the main.py file in the sqlite_archive module as the first file). There is also a test.py file that you can use to run this straight from the project directory (usually for testing purposes). It just imports the main module from the sqlite_archive library and runs the main function, nothing too complicated about that. Because of the modularization of this project, setup.py is probably the easier option.

---

If you don't specify a table name on the command line, it will use the first entry in the files list as the basis for the table name, replacing any periods, apostrophes, or spaces with underscores (SQL places special significance on these characters, so using them in the table name will freak the query processor and/or python out) it will also remove any commas for similar reasons. It will attempt to remove the file extension from the file name if the first entry on the files list is a file, but if your file has multiple extensions, it will only strip the first one (at least until I can figure out some way to strip all file extensions). This operation is case insensitive, so however you put the folder/file on the command line is how its going to derive the table name.

Similarly, when extracting files, if you don't specify an output directory, it will infer one based on the name of the table, replacing any underscores in the table name with spaces.
