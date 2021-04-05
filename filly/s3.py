# -*- coding: utf-8 -*-
"""
Description:
    This class performs all file operations with Amazon Simple Storage Service
    (Bucket).
Usage:
    # Creds should be set via the environment variable
    from s3 import S3
    s3 = S3()
"""

# pylint: disable=no-value-for-parameter

import os
import boto3
import logging
from tqdm import tqdm

logger = logging.getLogger('filly.s3')


class S3:

    def __init__(self, bucket):
        self.s3_client = boto3.client('s3')
        self.bucket = bucket

    def get_blob_references_from_s3(self, remote_dir) -> zip:
        """ Download all files from the remote directory to local, using
        the same file structure as the remote.

        """

        next_token = ''
        keys = []
        base_kwargs = {
            'Bucket': self.bucket,
            'Prefix': remote_dir,
        }

        while next_token is not None:
            kwargs = base_kwargs.copy()
            if next_token != '':
                kwargs.update({'ContinuationToken': next_token})
            results = self.s3_client.list_objects_v2(**kwargs)
            contents = results.get('Contents')

            keys += [content.get('Key') for content in contents]
            next_token = results.get('NextContinuationToken')

        return keys

    def get_all_from_s3(self, remote_dir: str):
        """ Download all files from the remote directory to local, using
        the same file structure as the remote.

        """

        blobs = self.get_blob_references_from_s3(self.bucket, remote_dir)

        logger.info(f'Start downloading {len(blobs)} files from S3.')

        for blob in tqdm(blobs, desc='From S3: ', total=len(blobs)):

            if not os.path.exists(os.path.dirname(blob)):
                os.makedirs(os.path.dirname(blob))

            self.s3_client.download_file(
                self.bucket,
                blob,
                blob
            )

        logger.info(f'Completed download of ALL image files.')

    def get_from_s3(self, remote_dir, local_dir, filename):

        current_fpath = os.path.join(local_dir, filename)
        self.s3_client.download_file(
            self.bucket,
            os.path.join(remote_dir, filename),
            current_fpath
        )
        logger.info(f'Completed file download to {current_fpath}')

    def put_to_s3(self, local_dir, remote_dir, filename):

        upload_fpath = os.path.join(remote_dir, filename)
        self.s3_client.upload_file(
            os.path.join(local_dir, filename),
            self.bucket,
            upload_fpath
        )

        logger.info(
            f'Completed file upload to s3://{self.bucket}/{upload_fpath}'
        )
