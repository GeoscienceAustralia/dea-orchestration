'use strict';
const SSH = require('simple-ssh');
const _ = require('lodash');
const AWS = require("aws-sdk");
AWS.config.update({region: 'ap-southeast-2'});

const ssm = new AWS.SSM();

const hostkey = process.env.hostkey;
const userkey = process.env.userkey;
const pkey = process.env.pkey;
const dea_module = process.env.DEA_MODULE;
const project = process.env.PROJECT;
const queue = process.env.QUEUE;

let req = {
    Names: [hostkey, userkey, pkey],
    WithDecryption: true
};
let keys = ssm.getParameters(req).promise();

function create_execution_string(event) {
    let args = _(event.args)
        .transform((r, v, k) => r['--' + k.replace('_', '-')] = `'${v}'`)
        .toPairs().flatten().join(' ');
    return event.command + ' ' + args;
}
// See https://hackernoon.com/you-should-use-ssm-parameter-store-over-lambda-env-variables-5197fc6ea45b
// For a more rigorous way of loading data from SSM using KSM
// Or look at https://github.com/n1ru4l/ssm-parameter-env

module.exports.execute_ingest = (event, context, callback) => {
    let product = event['product'];
    let year = event['year'];

    if (typeof year !== 'string') {
        return callback('Year not specified');
    }
    if (typeof product !== 'string') {
        return callback('Product not specified');
    }

    keys.then((data) => {
        let params = {};
        for (let p of data.Parameters) {
            params[p.Name] = p.Value;
        }

        let ssh = new SSH({
            host: params[hostkey],
            user: params[userkey],
            key: params[pkey],
        });

        let command = `execute_ingest --dea-module ${dea_module} --project ${project} --queue ${queue} --product ${product} --year ${year}`;
        console.log(`Executing: ${command}`);
        ssh.exec(command, {
            out: function(stdout) {
                console.log(stdout);
                const response = { statusCode: 200, body: 'Ingestion queued.' };
                callback(null, response);
            },
            err: function(stderr) {
                callback(`SSH Failed ${stderr}`);
            }
        }).start();
    });

};
