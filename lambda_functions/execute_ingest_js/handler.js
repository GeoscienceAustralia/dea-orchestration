'use strict';
const SSH = require('simple-ssh');

const AWS = require("aws-sdk");
AWS.config.update({region: 'ap-southeast-2'});

const ssm = new AWS.SSM();

let hostkey = 'orchestrator.raijin.users.default.host';
let userkey = 'orchestrator.raijin.users.default.user';
let pkey = 'orchestrator.raijin.users.default.pkey';

// See https://hackernoon.com/you-should-use-ssm-parameter-store-over-lambda-env-variables-5197fc6ea45b
// For a more advanced way of loading data from SSM using KSM
// Or look at https://github.com/n1ru4l/ssm-parameter-env

module.exports.execute_ingest = (event, context, callback) => {
    let dea_module = process.env.DEA_MODULE;
    let project = process.env.PROJECT;
    let queue = process.env.QUEUE;
    let product = event['product'];
    let year = event['year'];

    if (typeof year !== 'string') {
        return callback('Year not specified');
    }
    if (typeof product !== 'string') {
        return callback('Product not specified');
    }

    let req = {
        Names: [hostkey, userkey, pkey],
        WithDecryption: true
    };
    ssm.getParameters(req, function(err, data) {

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
