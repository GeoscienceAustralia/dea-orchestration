#!/usr/bin/env bash

set -euo pipefail
IFS=$'\n\t'

#rand_string=$(openssl rand -hex 6)
MINICONDA_PATH="/g/data/v10/private/miniconda3" # -$(rand_string)

rm -rf "$MINICONDA_PATH"
curl -o "$TMPDIR/miniconda.sh" https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
chmod +x "$TMPDIR/miniconda.sh"
cd "$TMPDIR" || exit
./miniconda.sh -b -f -u -p "$MINICONDA_PATH"
"$MINICONDA_PATH"/bin/conda update -y -c conda-forge --all

if [[ ! -d "$HOME"/.nvm ]]
then
    # Install Node Version Manager...
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.35.3/install.sh | bash
else
    echo "$HOME/.nvm directory already exists"
fi

chmod -R ug+rwx "$MINICONDA_PATH"
