# Cloud scripts

## Description
"Never spend 6 minutes doing something by hand when you can spend 6 hours failing to automate it" (c) Zhuowei Zhang

To get a list of arguments required for script execution, run script with `-h` flag(e.g. `python3/{script name} -h(--help)`).

### Set up a virtual environment
It is recomended to operate in a virtual environment, created in root folder:

* Install virtual environment module \
`python3 -m pip install virtualenv`
* Create virtual environment in folder \
`python3 -m virtualenv venv`
* Activate environment \
`source ./venv/bin/activate`
* Install dependencies \
`pip install -r requirements.txt`

### Scripts tldr:
* Amazon Web Services
    * ##### [[AWS] Switch Role](aws/sts-switch-role.py)
        **Description**: Script allows to assume IAM role faster, than typing sts commands and providing credentials to file manually. Script uses subprocess to enn commands instead of SDK, which allows to put it in system aliases and don't worry about managing modules.  

    * ##### [[AWS] Delete all KMS keys](aws/kms-delete-keys.py)
        **Description**: Script deletes all Custom Managed [KMS](https://aws.amazon.com/kms/) keys in given region.  

    * ##### [[AWS] Cognito delete all users](aws/cognito-delete-users.py)
        **Description**: Script removes ALL users from [AWS Cognito](https://aws.amazon.com/cognito/) User Pool in given region.
    
    * ##### [[AWS] Cognito extract all users](aws/cognito-extract-users.py)
        **Description**: Script downloads all users from [AWS Cognito](https://aws.amazon.com/cognito/) User Pool in given region and saves to JSON file. Has optional functionality to download users from [DynamoDB](https://aws.amazon.com/dynamodb/) in case it's used as backup for Cognito.

    * ##### [[AWS] Lambda Delete all versions](aws/lambda-delete-versions.py)
        **Description**: Script removes all [AWS Lambda](https://aws.amazon.com/lambda/) versions except for $LATEST version in given region.  
    
    * ##### [[AWS] SQS Extract all messages](aws/sqs-extract-messages.py)
        **Description**: Script extracts all messages from [SQS](https://aws.amazon.com/sqs/) queue. Script does not delete messages, so make sure to increase `visibility timeout` in queue settings to avoid getting same messages. 
