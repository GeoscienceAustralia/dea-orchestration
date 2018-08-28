#!/bin/bash

#MINICONDA_PATH=/g/data/v10/private/miniconda3-new

#rm -rf "$MINICONDA_PATH"
#curl -o "$TMPDIR/miniconda.sh" https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
#chmod +x "$TMPDIR/miniconda.sh"
#cd "$TMPDIR" || exit
#./miniconda.sh -b -f -u -p "$MINICONDA_PATH"
#"$MINICONDA_PATH"/bin/conda update -y -n base conda
#"$MINICONDA_PATH"/bin/conda install -y --override-channels -c conda-forge nodejs jupyterlab
#"$MINICONDA_PATH"/bin/jupyter lab build
#chmod -R ug+rw "$MINICONDA_PATH"

rm -rf /g/data/v10/private/miniconda3-new
curl -o "$TMPDIR/miniconda.sh" https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
chmod +x "$TMPDIR/miniconda.sh"
cd "$TMPDIR" || exit
./miniconda.sh -b -f -u -p /g/data/v10/private/miniconda3-new
/g/data/v10/private/miniconda3-new/bin/conda install -y --override-channels -c conda-forge nodejs jupyterlab=0.34.2
/g/data/v10/private/miniconda3-new/bin/jupyter lab build
#/g/data/v10/private/miniconda3/bin/conda update -y -n base conda
