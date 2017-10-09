#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu
set -x
{
    LINT_ARGS=(lambda_modules/*/* $(find raijin_scripts/. lambda_functions/. -iname '*.py'))
} &> /dev/null

export PYTHONPATH=$PWD/lambda_modules/dea_es:$PWD/lambda_modules/dea_raijin${PYTHONPATH:+:${PYTHONPATH}}

# Python linting
python3 -m pep8 "${LINT_ARGS[@]}"

python3 -m pylint -j 2 --reports no "${LINT_ARGS[@]}"

# Shell linting
shellcheck -e SC1071,SC1090,SC1091 scripts/*
shellcheck -e SC1071,SC1090,SC1091 raijin_scripts/*/run
shellcheck -e SC1071,SC1090,SC1091 raijin_scripts/*/*.sh

# Run tests, taking coverage.
# Users can specify extra folders as arguments.
python3 -m pytest -r sx --doctest-ignore-import-errors --durations=5 lambda_functions lambda_modules "$@"

set +x

# Optinally validate example yaml docs.
if which yamllint;
then
    set -x
    yamllint "$(find . \( -iname '*.yaml' -o -iname '*.yml' \) )"
    set +x
fi
