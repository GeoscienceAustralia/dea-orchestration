#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu
set -x
{
    LINT_ARGS=(lambda_modules/*/* $(find raijin_scripts lambda_functions ! -path '*node_modules*' -name '*.py'))
} &> /dev/null

export PYTHONPATH=$PWD/lambda_modules/dea_es:$PWD/lambda_modules/dea_raijin${PYTHONPATH:+:${PYTHONPATH}}

# Python linting
pycodestyle "${LINT_ARGS[@]}"

#pylint -j 2 --reports no "${LINT_ARGS[@]}"

# Finds shell scripts based on #!
find scripts raijin_scripts -type f -exec file {} \; | grep "Bourne-Again shell" | cut -d: -f1 | xargs -n 1 shellcheck -e SC1071,SC1090,SC1091

# Run tests, taking coverage.
# Users can specify extra folders as arguments.
pytest -r sx --doctest-ignore-import-errors --durations=5 lambda_functions lambda_modules "$@"

set +x

# Optinally validate example yaml docs.
if which yamllint;
then
    set -x
    yamllint "$(find . \( -iname '*.yaml' -o -iname '*.yml' \) )"
    set +x
fi
