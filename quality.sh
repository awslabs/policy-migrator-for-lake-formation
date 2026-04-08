#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo "Running Pytest"

OUT=$(poetry run coverage run -m pytest 2>&1)
RET=$?
if [ $RET -ne 0 ]; then
    echo "Test cases failed (${RET})." >&2
    echo "${OUT}" >&2
    exit 1
fi

echo "Test Coverage Report"
poetry run coverage report -m

# Run pylint checks
echo "Running Pylint"

OUT=$(poetry run pylint aws_resources config lakeformation_committers lakeformation_utils permissions policy_filters policy_readers post_processing_plugins tests 2>&1)
RET=$?
if [ $RET -ne 0 ]; then
    echo "Invalid Python code detected by pylint (${RET})." >&2
    echo "${OUT}" >&2
    exit 1
fi

# Run bandit checks
echo "Running bandit"

OUT=$(poetry run bandit -c pyproject.toml -r aws_resources config lakeformation_committers lakeformation_utils permissions policy_filters tests 2>&1)
RET=$?
if [ $RET -ne 0 ]; then
    echo "Invalid Python code detected by bandit (${RET})." >&2
    echo "${OUT}" >&2
    exit 1
fi

