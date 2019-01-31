#!/usr/bin/env node
'use strict';
const SSH = require('simple-ssh');

var _ = require('lodash');
var sleep = require("deasync").sleep;

// Configure the SDK for Node.js
const AWS = require("aws-sdk");
AWS.config.update({region: 'ap-southeast-2'});

const ssm = new AWS.SSM();

const hostkey = process.env.hostkey;
const userkey = process.env.userkey;
const pkey = process.env.pkey;
var CMDList = [];

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
        CMDList = [];  // Empty command variable before starting command execution

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

            let cmd = create_execution_string(event);
            CMDList.push(cmd)

            _.each(CMDList, function(this_command, i){
               ssh.exec(this_command, {
                  exit: (code, stdout, stderr) => {
                       var response = { statusCode: code, body: 'SSH command executed.' };

                       if (code == 0) {
                                        console.log("Executing command", i+1, "/", CMDList.length)
                                        console.log("$ " + this_command);

                                        // Return success with information back to the caller
                                        callback(null, response);

                                        console.log(response);
                                        sleep(500); // Sleep for 0.5 second
                       } else {
                               console.log(`STDERR: ${stderr}`);
                               response = { statusCode: code,
                                            body: `Failed to execute, ${this_command}, command` };

                               //  Return error with error information back to the caller
                               callback(`Failed to execute, ${this_command}, command`);
                       }
                  }
               });
            });
            ssh.start({
                       success: () => console.log(`Successfully connected to ${params[hostkey]}`),
                       fail: (err) => console.log(`Failed to connect to ${params[hostkey]}: ${err}`)
            });
        });
};
