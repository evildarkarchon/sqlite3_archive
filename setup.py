from setuptools import setup, find_packages
import src.sqlite_archive
setup(
    name="sqlite_archive",
    packages=find_packages('src'),
    package_dir={'':'src'},
    version='1.2',
    entry_points={'console_scripts': ['sqlite_archive = sqlite_archive.main:main']}
)