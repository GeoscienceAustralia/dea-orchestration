#!/usr/bin/env node
'use strict';
const SSH = require('simple-ssh');
var _ = require('lodash');

// Configure the SDK for Node.js
const AWS = require("aws-sdk");
AWS.config.update({region: 'ap-southeast-2'});

const ssm = new AWS.SSM();

const hostkey = process.env.hostkey;
const userkey = process.env.userkey;
const pkey = process.env.pkey;

/**
 * Turn an event into an ssh execution string.
 *
 * Compiles JavaScript templates into functions that can
 * can interpolate values, using <%= .. %>.
 *
 * Template object (event) is interpolated within the
 * template string (environment variable "cmd")
 *
 * Eg.
 *     templateString: 'mycommand --arg1 <%= code %>'
 *     templateObject: 'code: myarg'
 *
 *   Will result in a string of:
 *      'mycommand --arg1 myarg'
 */
function create_execution_string(event) {
    var compiled = _.template(process.env.cmd);
    return compiled(event);
}

exports.execute_ssh_command = (event, context, callback) => {
        let req = {
                   Names: [hostkey, userkey, pkey],
                   WithDecryption: true
        };
        let keys = ssm.getParameters(req).promise();

        keys.catch(function(err) {
            console.log(err);
            callback(`Error loading SSM keys: ${err}`);
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

            ssh
               .exec(command, {
                     exit: (code, stdout, stderr) => {
                        if (code == 0) {
                                         console.log(`Executing: ${command}`);
                                         console.log(`STDOUT: ${stdout}`);
                                         console.log(`SSH returncode: ${code}`);
                                         const response = { statusCode: 0, body: 'SSH command executed.' };
                                         // Return success with information back to the caller
                                         callback(null, response);
                        } else {
                                   console.log(`STDERR: ${stderr}`);
                                   console.log(`SSH returncode: ${code}`);
                                   //  Return error with error information back to the caller
                                   callback(`Failed to execute SSH command, ${stderr}`);
                        }
                     }
                   })
               .start({
                   success: () => console.log(`Successfully connected to ${params[hostkey]}`),
                   fail: (err) => console.log(`Failed to connect to ${params[hostkey]}: ${err}`)
               });
        });
};
