#!/bin/bash

rm -rf /g/data/v10/private/miniconda3
curl -o "$TMPDIR/miniconda.sh" https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
chmod +x "$TMPDIR/miniconda.sh"
cd "$TMPDIR"
./miniconda.sh -b -f -u -p /g/data/v10/private/miniconda3
/g/data/v10/private/miniconda3/bin/conda update -y -n base conda
