# -*- coding: utf-8 -*-
"""
Filly Module
-------------------------
This class handles the JSON file objects.
"""

import os
import json
from pathlib import Path
import logging
import subprocess
from tqdm import tqdm
import pandas as pd
from .s3 import S3

class FileUploadError(Exception):
    def __init__(self, filename, filepath, message=None):
        self.filename = filename
        self.filepath = filepath
        self.message = message

    def __str__(self):
        return 'Upload failed for File {} upload to {}. {}'.format(
            self.filename,
            self.filepath,
            None
        )


class Filly():
    """A class to read and write various data types"""

    def __init__(self, filename=None, filepath='./', data=None, remote=None, bucket_name=None):
        """Class to handle the reading, writing, transpose and syncing of various file types

        Arguments:
            filename (str): filename, without the file path
            filepath (str): filepath, without the actual filename
            mode (str): default to None, available options are
                - r: read data from filepath, filename. If this mode is set, the
                    data can be accessed via the normal accessor as Filly.data
                - w: write data to filepath, filename. If this mode is set, the
                    data must also be supplied when instantiating the object
                - None: an object holding just the file path and name
            data ([dict, pd.DataFrame]): data to be written or transposed. Only need to be
                provided if mode is "w"
            remote (str): default to None. If supplied, the data will be uploaded to
                the associated remote directory under the given filename

        Attributes:
            logger (logging.Logger): class logger
            filename (str): filename, without the filepath
            fullpath (str): full file path plus the file name
            data ([dict, pd.DataFrame]): the read data if mode is "r", otherwise not available
            s3 (s3.S3): the S3 class that handles s3 up/downloading

        .. todo::
            Add chunking, progress bar and all that goodness for read/write pandas
        """

        self.logger = logging.getLogger('filly')
        self.__set_path(filepath, filename)
        self.remote = remote

        if remote not in ['s3', 'hdfs', None]:
            raise ValueError(f'Invalid remote {remote}. Only `hdfs` or `s3` are supported.')

        if remote == 's3':
            if bucket_name not in [None, '']:
                self.s3 = S3(bucket=bucket_name)
            else:
                raise ValueError(f'Please supply your s3 bucket name.')

    def __set_path(self, filepath, filename, fullpath=None):
        if fullpath is not None:
            self.fullpath = fullpath
            self.filepath = os.path.dirname(fullpath)
            self.filename = os.path.basename(fullpath)

        if filename is not None:
            self.filename = filename
            if filepath is not None:
                self.fullpath = os.path.join(filepath, filename)

    def __run_cmd(self, args_list):
        """ Run linux commands """

        self.logger.info('Running system command: {0}'.format(' '.join(args_list)))
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s_output, s_err = proc.communicate()
        s_return = proc.returncode

        return s_return, s_output, s_err

    def write_data(self, data, filepath=None, filename=None, fullpath=None):

        self.__set_path(filepath, filename, fullpath)

        if not os.path.exists(filepath):
            os.makedirs(filepath, exist_ok=True)

        self._read_or_write(mode='w', data=data)

        if remote == 'hdfs':
            self._upload_to_hdfs(hdfs_dir)
        elif remote == 's3':
            self.s3.put_to_s3(filepath, filepath, filename)
        elif remote is None:
            pass
        else:
            raise ValueError(f'Invalid remote {remote}. Only `hdfs` or `s3` are supported.')

    def read_data(self, filepath=None, filename=None, fullpath=None):

        self.__set_path(filepath, filename, fullpath)

        # Download data if it is in a remote location
        if self.remote == 'hdfs':
            self._download_from_hdfs(hdfs_dir)
        elif self.remote == 's3':
            self.s3.get_from_s3(filepath, filepath, filename)
        else:
            pass;

        self._read_or_write(mode='r', data=None)


    def _upload_to_hdfs(self, hdfs_dir):
        """Upload local file to HDFS, replace if exists."""

        ret, _, err = self.__run_cmd(['hdfs', 'dfs', '-test', '-e', hdfs_dir])

        if not ret:
            ret, _, err = self.__run_cmd(['hdfs', 'dfs', '-rm', hdfs_dir])

        ret, _, err = self.__run_cmd(['hdfs', 'dfs', '-copyFromLocal', self.fullpath, hdfs_dir])

        if err:
            self.logger.error(err)
        else:
            self.logger.info(f'File {self.fullpath} uploaded successfully to {hdfs_dir}.')

    def _download_from_hdfs(self, hdfs_dir):
        """Upload local file to HDFS, replace if exists."""

        if os.path.isfile(self.fullpath):
            os.remove(self.fullpath)

        _, _, err = self.__run_cmd([
            'hdfs', 'dfs', '-copyToLocal', os.path.join(hdfs_dir, self.filename), self.fullpath
        ])
        if err:
            self.logger.error(err)
        else:
            self.logger.info(f'File {self.fullpath} downloaded successfully from {hdfs_dir}.')

    def _read_or_write(self, mode, data):
        """Define file handler to read or write the data based on input mode"""

        file_extension = Path(self.filename).suffix

        if bool(mode):
            if file_extension == '.json':
                self._json_handler(mode, data)

            elif file_extension == '.csv':
                self._csv_handler(mode, data)

            elif file_extension in ['.pkl', '.pickle']:
                self._pickle_handler(mode, data)

            else:
                raise TypeError('File type: {file_extension} not supported')

    def _json_handler(self, mode, data):
        """Either read json data as dictionary, or save dictionary as json"""

        if mode == 'r':
            self.data = json.loads(open(self.fullpath, "r").read())

        elif mode == 'w':
            with open(self.fullpath, 'w') as json_file:
                json.dump(data, json_file)
            self.logger.info(f'Json data saved at {self.fullpath}')

    def _csv_handler(self, mode, data):
        """Either read csv as pandas dataframe, or write pandas dataframe as csv"""

        if mode == 'r':
            self.data = pd.read_csv(self.fullpath)

        elif mode == 'w':
            data.to_csv(self.fullpath, index=False)
            self.logger.info(f'Pandas data saved at {self.fullpath}')

    def _pickle_handler(self, mode, data):
        """Either read pickle as pandas dataframe, or write pickle dataframe as csv"""

        if mode == 'r':
            self.data = pd.read_pickle(self.fullpath)

        elif mode == 'w':
            data.to_pickle(self.fullpath)
            self.logger.info(f'Pandas data saved at {self.fullpath}')

    def write_output(self, data, filename=None, filepath=None, fullpath=None):

        self.__set_path(filepath, filename, fullpath)

        with open(self.fullpath, 'w') as results_file:
            results_file.write(data)

    def read_input(self, filename=None, filepath=None, fullpath=None):

        output = ''
        self.__set_path(filepath, filename, fullpath)

        with open(self.fullpath, 'r') as results_file:
            for line in results_file:
                output += line

        return output