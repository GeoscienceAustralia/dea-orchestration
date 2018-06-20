'use strict';
const SSH = require('simple-ssh');
const _ = require('lodash');
const AWS = require("aws-sdk");
AWS.config.update({region: 'ap-southeast-2'});

const ssm = new AWS.SSM();

const hostkey = process.env.hostkey;
const userkey = process.env.userkey;
const pkey = process.env.pkey;
const pphrase = process.env.pphrase;

function create_execution_string(event) {
    /* Turn an event into an ssh execution string.
      Excepts event['command'] to be the command name,
      and all other elements to be named cli arguments. eg:
      let event = { command: 'mycommand', arg1: 'myarg' };
      Will result in a command of:
      'mycommand --arg1 myarg'
     */
    let command_name = event['command'];
    delete event['command'];
    let args = _(event.args)
        .transform((r, v, k) => r['--' + k.replace('_', '-')] = `'${v}'`)
        .toPairs().flatten().join(' ');
    return command_name + ' ' + args;
}

// See https://hackernoon.com/you-should-use-ssm-parameter-store-over-lambda-env-variables-5197fc6ea45b
// For a more advanced way of loading data from SSM using KSM
// Or look at https://github.com/n1ru4l/ssm-parameter-env
module.exports.execute_ingest = (event, context, callback) => {
        let req = {
                   Names: [hostkey, userkey, pkey, pphrase],
                   WithDecryption: true
        };
        let keys = ssm.getParameters(req).promise();

        keys.catch(function(err) {
            console.log(err);
        });
        keys.then((data) => {
            let params = {};
            for (let p of data.Parameters) {
                 params[p.Name] = p.Value;
            }
            console.log(`Host key: ${params[hostkey]}`);
            console.log(`User key: ${params[userkey]}`);
            let ssh = new SSH({
                               host: params[hostkey],
                               user: params[userkey],
                               key: params[pkey],
                               pass: params[pphrase],
                               });

            let command = create_execution_string(event);
            console.log(`Executing: ${command}`);
            ssh.exec(command, {
                               out: function(stdout) {
                               console.log(stdout);
                               const response = { statusCode: 200, body: 'SSH command executed.' };
                               callback(null, response);
                                   },
                               err: function(stderr) {
                                         callback(`SSH Failed ${stderr}`);
                                     }
                               }).start();
                             });
        };