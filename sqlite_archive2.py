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