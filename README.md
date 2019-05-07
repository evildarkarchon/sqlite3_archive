# sqlite3_archive

Python version of my sqlite3_archive project. 

This script's purpose is to store files as BLOBs in an sqlite3 database, along with a couple pieces of metadata (file name and sha256 hash). More metadata will be added as needed. The schema is not frozen yet, so newer versions of this script may be incompatible with older databases. Luckily, there are plenty of tools (including the original sqlite3 command line program) that can read sqlite3 databases.

This script requires at least version 3.7 of python because it takes advantage of features added in that version.

This program by default will print a list of files that failed the UNIQUE constraint along with the file it conflicted with. It will also by default store said duplicates list in a json file named duplicates.json (by default, although you can change the location). If you leave the ability to save the json file and you keep using the same file name, you can keep track of duplicates from other directories as well.