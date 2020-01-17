#!/usr/bin/env node
'use strict'
const SSH = require('simple-ssh')

var _ = require('lodash')

// Configure the SDK for Node.js
const AWS = require('aws-sdk')
AWS.config.update({ region: 'ap-southeast-2' })

const ssm = new AWS.SSM()

const hostkey = process.env.hostkey
const userkey = process.env.userkey
const pkey = process.env.pkey

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
function create_execution_string (event) {
  var compiled = _.template(process.env.cmd)
  return compiled(event)
}

exports.execute_ssh_command = (event, context, callback) => {
  let req = {
    Names: [hostkey, userkey, pkey],
    WithDecryption: true
  }
  let keys = ssm.getParameters(req).promise()

  keys.catch(function (err) {
    console.log(err)
    callback(`Error loading SSM keys: ${err}`)
  })

  keys.then((data) => {
    let params = {}
    for (let p of data.Parameters) {
      params[p.Name] = p.Value
    }

    console.log(`Host: ${params[hostkey]}`)
    console.log(`User: ${params[userkey]}`)
    let cmd = create_execution_string(event)

    console.log('Executing command: ', cmd)

    let ssh = new SSH({
      host: params[hostkey],
      user: params[userkey],
      key: params[pkey],
    })

    // Queue up our command and handlers
    ssh.exec(cmd, {
      out: (stdout) => console.log(`STDOUT: ${stdout}`),
      err: (stderr) => console.log(`STDERR: ${stderr}`),
      exit: (code, stdout, stderr) => {
        let response = { statusCode: code, body: 'SSH command executed.' }

        console.log(response)
        if (code === 0) {
          // Command completed successfully
          callback(null, response)
        } else {
          // I'm not sure we want to return an error from the lambda in this case.
          // AWS will attempt to re-run.
          callback(Error(`Failed to execute "${cmd}. Return code: ${code}`))
        }
      }
    }).start({
      success: () => console.log(`Connected to ${params[hostkey]}`),
      fail: (err) => callback(Error(`Failed to connect to ${params[hostkey]}: ${err}`))
    })
  })
}
