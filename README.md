# sqlite3_archive

Python version of my sqlite3_archive project. This script requires at least version 3.7 of python because it takes advantage of features added in that version. I only test this script on Windows since that's the operating system of the machine I write this script on. I try to keep Windows specific stuff out of the script and I use pathlib to deal with all of the path manipulation, so it should work just fine on both Unix/Linux and Windows platforms.

---

This script's purpose is to store files as BLOBs in an sqlite3 database, along with a couple pieces of metadata (file name and sha512 hash). More metadata will be added as needed. The schema is not frozen yet, so newer versions of this script may be incompatible with older databases. Luckily, there are plenty of tools (including the original sqlite3 command line program) that can read sqlite3 databases.

---

By default, this script will print a list of files that failed the UNIQUE constraint along with the file it conflicted with. It will also store said duplicates list in a json file named duplicates.json (File location is changeable on the command line). If you leave the ability to save the json file enabled and you keep using the same file name, you can keep track of duplicates from other directories as well. Obviously, this will only show duplicate files that were put in the same table, duplicate files may still exist elsewhere in the database if you use multiple tables (which you should). By default, the json keys will be relative to the parent directory of the file specified, but there is an option to have it put the full path of the conflicting file in the json key. Putting a full path to the original file is obviously not possible because that information is not stored in the database. Also, the list is stored in a key that is consists of the database's path (relative or full).

---

If you use this on Windows, I suggest using [PyInstaller](https://pypi.org/project/PyInstaller/) to pack this script into an executable so that you don't have to invoke python on the script every time you want to use it. I also suggest using its --onefile option so that you don't have a bunch of dlls cluttering up the directory, but that's personal preference. After all that, just put the executable (and other files in the dist directory, if you don't use the --onefile option) into a directory and add that directory to your PATH environment variable and you can just execute it by name whenever you want to use it.

---

If you don't specify a table name on the command line, it will use the first entry in the files list as the basis for the table name, replacing any periods, apostrophes, or spaces with underscores (SQL places special significance on these characters, so using them in the table name will freak the query processor and/or python out) it will also remove any commas for similar reasons. It will attempt to remove the file extension from the file name if the first entry on the files list is a file, but if your file has multiple extensions, it will only strip the first one (at least until I can figure out some way to strip all file extensions). This operation is case insensitive, so however you put the folder/file on the command line is how its going to derive the table name. Table name inference is only for add mode, it will not work for extract mode at this time.

Similarly, when extracting files, if you don't specify an output directory, it will infer one based on the name of the table, replacing any underscores in the table name with spaces.
