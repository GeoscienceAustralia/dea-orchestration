## Setup

Since this plugin uses the Serverless plugin `serverless-secrets-plugin` you need to setup the `node_modules` by running:

    1. cd ~/ from anywhere and then git clone git@github.com:creationix/nvm.git
    2.	cd ~/.nvm
    3.	git checkout <latest version from the git repo>
    4.	export NVM_DIR="$HOME/.nvm"
    5.	[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"  # This loads nvm
    6.	[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"  # This loads nvm bash_completion
    7.	. nvm.sh ------------- source to ~/.bashrc, or ~/.profile, or ~/.zshrc file
    8.	nvm --version
    9.	nvm install node
    10.	nvm use node
    11.	node --version
    12. nvm install npm
    13. npm --version
    14.	npm install -g serverless
    15.	serverless --version
    16. serverless config credentials --provider aws --key <AWS key> --secret <AWS Secret key>

## Installation before deployment

    1. npm install serverless-pseudo-parameters --save-dev
    2. npm install simple-ssh --save-dev
    3. serverless --version

## Deploy

In order to deploy the endpoint, simply run:

    serverless deploy --stage <ga-aws-dea or ga-aws-dea-dev> -v