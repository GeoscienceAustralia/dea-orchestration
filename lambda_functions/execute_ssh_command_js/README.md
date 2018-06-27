## Setup

Since this plugin uses the Serverless plugin `serverless-secrets-plugin` you need to setup the `node_modules` by running:

    1) Follow nvm installation guide as mentioned in https://github.com/creationix/nvm. 
    2) Source to ~/.bashrc, or ~/.profile, or ~/.zshrc file:
          `. nvm.sh`
    3) Run the following shell commands:
        a) `nvm --version`
        b) `nvm install node`
        c) `nvm use node`
        d) `node --version`
        e) `nvm install npm`
        f) `npm --version`
        g) `npm install -g serverless`
        h) `serverless --version`
        i) `serverless config credentials --provider aws --key <AWS key> --secret <AWS Secret key>`

## Installation before deployment

    1) Run the following shell commands:
        a) `npm install serverless-pseudo-parameters --save-dev`
        b) `npm install simple-ssh --save-dev`
        c) `serverless --version`
    2) Ensure that ssh private and public keys are generated on the Raijin system.
    3) Private key is copied to aws ssm parameter using the following aws command line:
           aws ssm put-parameter --name "orchestrator.raijin.users.default.pkey" --type "SecureString" --value "-----BEGIN RSA PRIVATE KEY-----
            MIIJJgIBAAKCAgEA3lkvu08KVjA7hWyFnKo+0Eb/S1SCZxIgwlfDAjlJexdQdh1y
            ..........
            MT7OOmFZxUVThbi1Hl6VLzA+cIImOVVrfvtfDlgvG9mNWVvONIVDeIma
            -----END RSA PRIVATE KEY-----" --overwrite
    4) Public key is copied to authorized_keys file
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

    1) `npm install`
    2) `serverless deploy --stage <dev or production> -v`
    
    Note: `dea-stacker submit` command only works from Raijin system and not VDI system.
    
## Invoke

In order to run the script (before an event is triggered):

     1) `serverless invoke -f execute_ingest -l -d '{"command": "execute_ingest", "year": "2017", "product": "ls8_nbar_albers", "dea-module": "dea/20180515", "project":"u46", "queue":"express"}'`