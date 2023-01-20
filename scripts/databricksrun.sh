#!/usr/bin/python3

#This is the original run script from databricks-cli used to interact from CLI.
# -*- coding: utf-8 -*-
import re
import sys

from databricks_cli.cli import cli

if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit(cli())
