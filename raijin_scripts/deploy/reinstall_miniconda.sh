#!/bin/bash

MINICONDA_PATH=/g/data/v10/private/miniconda3-new

rm -rf "$MINICONDA_PATH"
curl -o "$TMPDIR/miniconda.sh" https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
chmod +x "$TMPDIR/miniconda.sh"
cd "$TMPDIR" || exit
./miniconda.sh -b -f -u -p "$MINICONDA_PATH"
#"$MINICONDA_PATH"/bin/conda update -y -n base --all
"$MINICONDA_PATH"/bin/conda install -y -c conda-forge pip
"$MINICONDA_PATH"/bin/conda install -y -c anaconda memory_profiler
"$MINICONDA_PATH"/bin/conda install -y -c conda-forge ipywidgets widgetsnbextension 'nodejs<10' notebook nb_conda_kernels jupyterlab=0.33.0

#"$MINICONDA_PATH"/bin/conda install -c conda-forge --override-channels --yes python=3.6 pip cookiecutter=1.6 notebook=5.5 pandas=0.23 nodejs=9.11 jupyterlab bqplot ipyvolume pythreejs
#"$MINICONDA_PATH"/bin/jupyter labextension install @jupyter-widgets/jupyterlab-manager jupyter-threejs ipyvolume bqplot @jupyterlab/geojson-extension @jupyterlab/fasta-extension
#"$MINICONDA_PATH"/bin/conda install -y -c conda-forge yarn=1.6.0
chmod -R ug+rw "$MINICONDA_PATH"
