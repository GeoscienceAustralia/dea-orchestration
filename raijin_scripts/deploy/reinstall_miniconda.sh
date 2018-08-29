#!/bin/bash

MINICONDA_PATH=/g/data/v10/private/miniconda3-new

rm -rf "$MINICONDA_PATH"
curl -o "$TMPDIR/miniconda.sh" https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
chmod +x "$TMPDIR/miniconda.sh"
cd "$TMPDIR" || exit
./miniconda.sh -b -f -u -p "$MINICONDA_PATH"
"$MINICONDA_PATH"/bin/conda update -y -n base --all
"$MINICONDA_PATH"/bin/conda install -y -c conda-forge pip
chmod -R ug+rw "$MINICONDA_PATH"