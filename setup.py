from setuptools import setup, find_packages
import sqlite_archive
setup(
    name="sqlitearchive",
    packages=find_packages('src'),
    package_dir={'':'src'},
    version='1.0',
    entry_points={'console_scripts': ['sqlite_archive = sqlite_archive.sqlite_archive:main']}
)