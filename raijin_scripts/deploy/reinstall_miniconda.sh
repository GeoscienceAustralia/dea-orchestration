#!/usr/bin/env bash

MINICONDA_PATH=/g/data/v10/private/miniconda3-new

rm -rf "$MINICONDA_PATH"
curl -o "$TMPDIR/miniconda.sh" https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
chmod +x "$TMPDIR/miniconda.sh"
cd "$TMPDIR" || exit
./miniconda.sh -b -f -u -p "$MINICONDA_PATH"
"$MINICONDA_PATH"/bin/conda update -y -c conda-forge --all
"$MINICONDA_PATH"/bin/conda install -y -c conda-forge pip glueviz

if [ ! -d "$HOME"/.nvm ]
then
    # Git install Node Version Manager
    echo "Installing NVM"
    cd ~/ || exit
    git clone https://github.com/creationix/nvm.git .nvm
    cd ~/.nvm || exit
    git checkout v0.33.11
else
    echo "$HOME/.nvm directory already exists"
fi

cd ~/.nvm || exit
export NVM_DIR="$HOME/.nvm"

# shellcheck source=/dev/null
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh" # This loads nvm

# shellcheck source=/dev/null
[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"  # This loads nvm bash_completion

# shellcheck source=/dev/null
. "$NVM_DIR/nvm.sh"

git checkout master
git pull
nvm_version=$(nvm --version)
echo "NVM version = $nvm_version"
chmod -R ug+rwx "$MINICONDA_PATH"
