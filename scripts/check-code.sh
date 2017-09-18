#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu
set -x

python3 -m pep8 lambda_functions lambda_modules raijin_scripts --max-line-length 120

python3 -m pylint -j 2 --reports no lambda_modules lambda_functions raijin_scripts

# Run tests, taking coverage.
# Users can specify extra folders as arguments.
python3 -m pytest -r sx --doctest-ignore-import-errors --durations=5 lambda_functions lambda_modules $@

set +x

# Optinally validate example yaml docs.
if which yamllint;
then
    set -x
    yamllint $(find . \( -iname '*.yaml' -o -iname '*.yml' \) )
    set +x
fi
