#!/bin/bash

pip install awscli --upgrade --user
if [ ! -f ~/.aws/credentials ]; then
    echo "Please configure aws cli"
fi
