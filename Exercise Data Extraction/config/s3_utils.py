import boto3
import s3fs
import fsspec

from config.settings import aws_access_key_id, aws_secret_access_key


def get_s3_client_and_storage_options()-> tuple[boto3.client,dict]:

    """

    Return a boto3 S3 client and pandas storage option for s3fs
    """
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key,)

    storage_options = {
        "key" : s3._request_signer._credentials.access_key,
        "secret" : s3._request_signer._credentials.secret_key,
    }

    return s3, storage_options