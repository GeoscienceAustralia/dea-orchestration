## Setup

Since this plugin uses the Serverless plugin `serverless-secrets-plugin` you need to setup the `node_modules` by running:

    1) Follow nvm installation guide as mentioned in https://github.com/creationix/nvm. 
    2) cd to the folder where nvm.sh is installed and run the following command to source to ~/.bashrc, or ~/.profile, or ~/.zshrc file:
          `. nvm.sh`
    3) Run the following shell commands (to check if installation is completed):
        a) `nvm --version`
        b) `nvm install node`
        c) `nvm use node`
        d) `node --version`
        e) `nvm install npm`
        f) `npm --version`
        g) `npm install -g serverless`
        h) `serverless --version`
    4) Configure AWD configuration values such as AWS Access Key Id and AWS Secret Access Key using aws configure command.
        i) aws configure [--profile profile-name]
           Note: Configure two aws profiles, one named 'devProfile' and another named 'prodProfile'.
            
        Ref: https://docs.aws.amazon.com/cli/latest/reference/configure/index.html

## Installation before deployment

    1) Ensure that ssh private and public keys are generated on the Raijin system.
    2) Private key is copied to aws ssm parameter using the following aws command line:
           aws ssm put-parameter --name "orchestrator.raijin.users.default.pkey" --type "SecureString" --value file://~/.ssh/my_private_key_file --overwrite
    3) Public key is copied to authorized_keys file
        Note: Check if any temporary keys (if any) are there by firing the following command,
                  `ls -al ~/.ssh`
              If there are, then remove the temporary files.

## Setup Raijin scripts for automation

    In order to set up scripts library on Raijin, the user is required to generate 2 ssh keys.
       - One to be able to access the :bash:`remote` script
       - Another to be able to access the :bash:`git_pull` script (to limit how this is triggered)

    When adding public keys to :bash:`~/.ssh/authorized_keys`:
         The ssh key for the `remote` script should be prepended with the following bash command:
              :bash:`command="{{directory_location}}/scripts/remote",no-agent-forwarding,no-port-forwarding,no-pty,no-user-rc,no-X11-forwarding`
              
              Note: Replace the '{{directory_location}}' with the directory path.

         The ssh key for `git_pull` script should be prepended with the following bash command:
              :bash:`command="{{directory_location}}/scripts/git_pull",no-agent-forwarding,no-port-forwarding,no-pty,no-user-rc,no-X11-forwarding`
              
              Note: Replace the '{{directory_location}}' with the directory path.
              
          Also, rename the name of the ssh public key as follows:
             1) Remote_Script ssh public key: `== Running of scripts under NCI`
             2) Git_Pull ssh public key: `== Automated deployment of dea-orchestration`

## Deploy

In order to deploy the endpoint, simply run:

    1) cd to the folder where we have handler.js script, package.json and serverless.yml files.
    2) `npm install`
    
    To deploy on development environment, use:
    3) `sls deploy -v -s dev`

    To deploy on production environment, use:
    3) `sls deploy -v -s prod`
    
    Note: `dea-stacker submit` command only works from Raijin system and not VDI system.
    
## Invoke

In order to run the script (before an event is triggered), simply run:

     1) `sls invoke -f execute_sync -l -s prod -d '{"year": "2018", "product": "ls8_nbart_scene", "dea-module": "dea/20180801", "project":"v10", "queue":"express", "stage": "prod", "trasharchived": "no", "path": "/g/data/rs0/scenes/nbar-scenes-tmp/ls8/2018/07/output/nbart/"}'`
     
     Note: Use appropriate stage (-s dev or -s prod) when using sls invoke