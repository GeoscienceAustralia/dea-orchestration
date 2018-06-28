#!/usr/bin/env node
'use strict';
const SSH = require('simple-ssh');
var _ = require('lodash');
const AWS = require("aws-sdk");
AWS.config.update({region: 'ap-southeast-2'});

const ssm = new AWS.SSM();

const hostkey = process.env.hostkey;
const userkey = process.env.userkey;
const pkey = process.env.pkey;

/**
 * Turn an event into an ssh execution string.
 *
 * Excepts event['command'] to be the command name,
 * and all other elements to be named cli arguments. eg:
 *
 * let event = { command: 'mycommand', arg1: 'myarg' };
 *
 * Will result in a command of:
 * 'mycommand --arg1 myarg'
 */
function create_execution_string(event) {
    var compiled = _.template(process.env.cmd);
    return compiled(event);
}

exports.raijin_ssh_command = (event, context, callback) => {
        let req = {
                   Names: [hostkey, userkey, pkey],
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

            var ssh = new SSH({
                               host: params[hostkey],
                               user: params[userkey],
                               key: params[pkey],
                               });

            console.log(`Host key: ${params[hostkey]}`);
            console.log(`User key: ${params[userkey]}`);

            let command = create_execution_string(event);
            console.log(`Executing: ${command}`);

	        ssh.exec(command, {
                     exit: (code, stdout, stderr) => {
                        console.log(`STDOUT: ${stdout}`);
                        console.log(`STDERR: ${stderr}`);
                     }
                   })
               .start({
                   success: () => console.log(`Successfully connected to ${params[hostkey]} system.`),
                   fail: (err) => console.log(`Failed to connect to Raijin system: ${err}`)
               });
        });
};