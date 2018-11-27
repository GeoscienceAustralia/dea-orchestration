#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu
set -x
{
    readarray -t PY_FILES < <(find raijin_scripts lambda_functions ! -path '*node_modules*' -name '*.py')
    PY_FILES+=(lambda_modules/*/*)
} &> /dev/null

export PYTHONPATH=$PWD/lambda_modules/dea_es:$PWD/lambda_modules/dea_raijin${PYTHONPATH:+:${PYTHONPATH}}

# Python linting
pycodestyle "${PY_FILES[@]}"

pylint -j 2 --reports no "${PY_FILES[@]}"

# Finds shell scripts based on #!
readarray -t SHELL_SCRIPTS < <(find scripts raijin_scripts -type f -exec file {} \; | grep "Bourne-Again shell" | cut -d: -f1)
shellcheck -e SC1071,SC1090,SC1091 "${SHELL_SCRIPTS[@]}"

# Run tests, taking coverage.
# Users can specify extra folders as arguments.
pytest -r sx --doctest-ignore-import-errors --durations=5 lambda_functions "$@"

set +x

# Check the serverless configuration file
if command -v serverless;
then
    set +ex
    pushd "$PWD/lambda_functions/execute_ssh_command_js"
    _TMP="$(mktemp -d)"
    echo "writing temporary serverless artifacts to $_TMP"
    serverless package -s prod -p "${_TMP}"  # test prod setting
    _RET=$?
    rm -rf "${_TMP}"  # clean up test directory
    if [ $_RET -ne 0 ]; then echo "serverless failed to generate a package" && exit 1; fi
    set -ex
fi

# If yamllint is available, validate yaml documents
if command -v yamllint;
then
    set -x
    readarray -t YAML_FILES < <(find . \( -iname '*.yaml' -o -iname '*.yml' \) ! -path '*node_modules*' )
    yamllint "${YAML_FILES[@]}"
    set +x
fi
