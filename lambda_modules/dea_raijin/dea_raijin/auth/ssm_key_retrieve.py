import boto3

from ..config import AWS_REGION


SSM = boto3.client('ssm', AWS_REGION)


def get_ssm_parameter(name, with_decryption=True):
    """ Returns a parameter from the Secure Systems Manager

    Args:
        name (str): Key name from the ssm
        with_decryption (bool): if the value should be decrypted with default kms key

    Returns:
        str: The value of the key in the ssm

    Raises:
        AttributeError: If key doesn't exist in the ssm.
    """

    response = SSM.get_parameters(Names=[name], WithDecryption=with_decryption)

    if response:
        return response['Parameters'][0]['Value']  # Return the first value from doc stub
    raise AttributeError("Key '{}' not found in region '{}'".format(name, AWS_REGION))
