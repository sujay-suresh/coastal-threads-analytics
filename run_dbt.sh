#!/bin/bash
# Run dbt-core via Python 3.13 (dbt-fusion has LoadLibraryExW issue on Windows)
cd "$(dirname "$0")" || exit 1
"/c/Program Files/Python313/python.exe" -c "
import sys
from dbt.cli.main import dbtRunner
r = dbtRunner()
result = r.invoke(sys.argv[1:])
sys.exit(0 if result.success else 1)
" "$@"
