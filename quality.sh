#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

OUT=$(poetry run pytest 2>&1)
RET=$?
if [ $RET -ne 0 ]; then
    echo "Invalid Python code detected by pylint (${RET})." >&2
    echo "${OUT}" >&2
    exit 1
fi

# Run pylint checks
OUT=$(poetry run pylint src tests 2>&1)
RET=$?
if [ $RET -ne 0 ]; then
    echo "Invalid Python code detected by pylint (${RET})." >&2
    echo "${OUT}" >&2
    exit 1
fi

# Run bandit checks
OUT=$(poetry run bandit -c pyproject.toml -r aws_resources config lakeformation_committers lakeformation_utils permissions policy_filters tests 2>&1)
RET=$?
if [ $RET -ne 0 ]; then
    echo "Invalid Python code detected by bandit (${RET})." >&2
    echo "${OUT}" >&2
    exit 1
fi

