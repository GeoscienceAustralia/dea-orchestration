#!/usr/bin/env node
'use strict';
const SSH = require('simple-ssh');
const path = require('path');
var _ = require('lodash');
var sleep = require("deasync").sleep;

// Configure the SDK for Node.js
const AWS = require("aws-sdk");
AWS.config.update({region: 'ap-southeast-2'});

const ssm = new AWS.SSM();

const hostkey = process.env.hostkey;
const userkey = process.env.userkey;
const pkey = process.env.pkey;
var command_list = [];
var command = [];

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

/**
 * Construct sync path for Landsat Scenes within an event
 * and turn the entire event into an ssh execution command list.
 */
function process_ls_sync_command(event, bPath, suffix) {
    var yearRange = process.env.yearrange;
    var arr = yearRange.split("-");
    for(var year=arr[0]; year <= arr[1]; year++) {
        event.path = bPath + year + "/??" + suffix;
        event.year = year;
        let cmd = create_execution_string(event);
        command_list.push(cmd)
    }
    return command_list
}

/**
 * Construct sync path for Sentinel 2 ARD granules within an event
 * and turn the entire event into an ssh execution command list.
 *
 * The directory structure for S2 ARD granules is packaged into a single
 * directory (/g/data/if87/datacube/002/S2_MSI_ARD/packaged) making it
 * difficult for orchestration of past and future years. Hence this new function
 * to take care of this dependency.
 */
function process_s2ard_sync_command(event) {
    var basePath = process.env.basepath;
    var yearRange = process.env.yearrange;
    var arr = yearRange.split("-");
    var months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'];
    for(var year=arr[0]; year <= arr[1]; year++) {
        for(var j=0; j<months.length; j++) {
            event.path = basePath + year + "-" + months[j] + "-*/*/";
            event.year = year;
            let cmd = create_execution_string(event);
            command_list.push(cmd)
        }
    }
    return command_list
}

/**
 * Construct time range as a string.
 */
function event_range(year, month, date1, date2) {
    return "'" + year + "-" + month + '-' + date1 + ' < time < '+ year + "-" + month + '-' + date2 + "'";
}

/**
 * Construct cog conversion command and turn the entire event into an ssh execution command list.
 */
function process_cog_conv_command(event) {
    var yearRange = process.env.yearrange;
    var arr = yearRange.split("-");
    var months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'];
    for(var year=arr[0]; year <= arr[1]; year++) {
        for(var j=0; j<months.length; j++) {
            event.time_range = event_range(year, months[j], "01", "05");
            let command_1 = create_execution_string(event);
            command_list.push(command_1)
            event.time_range = event_range(year, months[j], "05", "10");
            command_1 = create_execution_string(event);
            command_list.push(command_1)
            event.time_range = event_range(year, months[j], "10", "15");
            command_1 = create_execution_string(event);
            command_list.push(command_1)
            event.time_range = event_range(year, months[j], "15", "20");
            command_1 = create_execution_string(event);
            command_list.push(command_1)
            event.time_range = event_range(year, months[j], "20", "25");
            command_1 = create_execution_string(event);
            command_list.push(command_1)
            event.time_range = event_range(year, months[j], "25", "30");
            command_1 = create_execution_string(event);
            command_list.push(command_1)
            event.time_range = event_range(year, months[j], "30", "31");
            command_1 = create_execution_string(event);
            command_list.push(command_1)
        }
    }
    return command_list
}

exports.execute_ssh_command = (event, context, callback) => {
        let req = {
                   Names: [hostkey, userkey, pkey],
                   WithDecryption: true
        };
        let keys = ssm.getParameters(req).promise();
        command_list = [];  // Empty command list before starting command execution
        command = [];  // Empty command variable before starting command execution

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

            if (event.product == 'ls8_nbar_scene') {
                 let bPath = process.env.ls8_nbar_nbart_basepath;
                 let suffix = process.env.nbar_suffix;
                 command = process_ls_sync_command(event, bPath, suffix);
            } else if (event.product == 'ls8_nbart_scene') {
                 let bPath = process.env.ls8_nbar_nbart_basepath;
                 let suffix = process.env.nbart_suffix;
                 command = process_ls_sync_command(event, bPath, suffix);
            } else if (event.product == 'ls7_nbar_scene') {
                 let bPath = process.env.ls7_nbar_nbart_basepath;
                 let suffix = process.env.nbar_suffix;
                 command = process_ls_sync_command(event, bPath, suffix);
            } else if (event.product == 'ls7_nbart_scene') {
                 let bPath = process.env.ls7_nbar_nbart_basepath;
                 let suffix = process.env.nbart_suffix;
                 command = process_ls_sync_command(event, bPath, suffix);
            } else if (event.product == 'ls8_pq_scene') {
                 let bPath = process.env.ls8_pq_basepath;
                 let suffix = process.env.pq_suffix;
                 command = process_ls_sync_command(event, bPath, suffix);
            } else if (event.product == 'ls7_pq_scene') {
                 let bPath = process.env.ls7_pq_basepath;
                 let suffix = process.env.pq_suffix;
                 command = process_ls_sync_command(event, bPath, suffix);
            } else if (event.product == 'ls8_pq_legacy_scene') {
                 let bPath = process.env.ls8_pq_legacy_basepath;
                 let suffix = process.env.pq_legacy_suffix;
                 command = process_ls_sync_command(event, bPath, suffix);
            } else if (event.product == 'ls7_pq_legacy_scene') {
                 let bPath = process.env.ls7_pq_legacy_basepath;
                 let suffix = process.env.pq_legacy_suffix;
                 command = process_ls_sync_command(event, bPath, suffix);
            } else if (event.product == 's2_ard_granule') {
                 command = process_s2ard_sync_command(event);
            } else if (!event.cog_product || event.cog_product === "") {
                 command = process_cog_conv_command(event);
            } else {
                 let cmd = create_execution_string(event);
                 command.push(cmd)
            }

            for (var i=0; i < command.length; i++) {
                sleep(1000);  // Delay for 1 second before executing new command
                ssh
                   .exec(command[i], {
                         exit: (code, stdout, stderr) => {
                            if (code == 0) {
                                            console.log(`Executing: ${command[i]}`);
                                            console.log(`STDOUT: ${stdout}`);
                                            console.log(`SSH returncode: ${code}`);
                                            const response = { statusCode: 0, body: 'SSH command executed.' };
                                            // Return success with information back to the caller
                                            callback(null, response);
                            } else {
                                     console.log(`STDERR: ${stderr}`);
                                     console.log(`SSH returncode: ${code}`);
                                     //  Return error with error information back to the caller
                                     callback(`Failed to execute, ${command[i]}, command`);
                            }
                         }
                   })
                .start({
                        success: () => console.log(`Successfully connected to ${params[hostkey]}`),
                        fail: (err) => console.log(`Failed to connect to ${params[hostkey]}: ${err}`)
                });
             }
        });
};
