import logging

import boto3
from botocore.exceptions import NoCredentialsError


# TODO 单例模式
class S3Client:
    def __init__(self):
        self.s3_client = boto3.client("s3")
        try:
            self.s3_client.list_buckets()
        except NoCredentialsError:
            raise Exception("No AWS credentials found")

    def upload_to_s3(self, file_path, s3_key, bucket_name="sing-strong", extra_args=None):
        self.s3_client.upload_file(
            file_path,
            bucket_name,
            s3_key,
            extra_args,
        )
        return s3_key
