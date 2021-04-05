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

    def __init__(self, filename=None, filepath='./', mode=None, data=None, remote=None, bucket_name=None):
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

        if filename is not None:
            if mode not in ['r', 'w', None]:
                raise ValueError(f'Invalid mode {mode}. mode can only be "r" or "w"')
            self.data = None if mode == 'r' else 'Data only available in "r" mode.'

            if remote == 's3':
                self.s3 = S3(bucket=bucket_name)

            if mode == 'r':
                if remote == 'hdfs':
                    self.download_from_hdfs(hdfs_dir)
                elif remote == 's3':
                    self.s3.get_from_s3(filepath, filepath, filename)
                elif remote is None:
                    pass
                else:
                    raise ValueError(f'Invalid remote {remote}. Only `hdfs` or `s3` are supported.')

            self.read_or_write(mode, data)

            if mode == 'w':
                if not os.path.exists(filepath):
                    os.makedirs(filepath, exist_ok=True)

                if remote == 'hdfs':
                    self.upload_to_hdfs(hdfs_dir)
                elif remote == 's3':
                    self.s3.put_to_s3(filepath, filepath, filename)
                elif remote is None:
                    pass
                else:
                    raise ValueError(f'Invalid remote {remote}. Only `hdfs` or `s3` are supported.')

    def __set_path(self, filepath, filename):
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

    def upload_to_hdfs(self, hdfs_dir):
        """Upload local file to HDFS, replace if exists."""

        ret, _, err = self.__run_cmd(['hdfs', 'dfs', '-test', '-e', hdfs_dir])

        if not ret:
            ret, _, err = self.__run_cmd(['hdfs', 'dfs', '-rm', hdfs_dir])

        ret, _, err = self.__run_cmd(['hdfs', 'dfs', '-copyFromLocal', self.fullpath, hdfs_dir])

        if err:
            self.logger.error(err)
        else:
            self.logger.info(f'File {self.fullpath} uploaded successfully to {hdfs_dir}.')

    def download_from_hdfs(self, hdfs_dir):
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

    def read_or_write(self, mode, data):
        """Define file handler to read or write the data based on input mode"""

        file_extension = Path(self.filename).suffix

        if bool(mode):
            if file_extension == '.json':
                self.json_handler(mode, data)

            elif file_extension == '.csv':
                self.csv_handler(mode, data)

            elif file_extension in ['.pkl', '.pickle']:
                self.pickle_handler(mode, data)

            else:
                raise TypeError('File type: {file_extension} not supported')

    def json_handler(self, mode, data):
        """Either read json data as dictionary, or save dictionary as json"""

        if mode == 'r':
            self.data = json.loads(open(self.fullpath, "r").read())

        elif mode == 'w':
            with open(self.fullpath, 'w') as json_file:
                json.dump(data, json_file)
            self.logger.info(f'Json data saved at {self.fullpath}')

    def csv_handler(self, mode, data):
        """Either read csv as pandas dataframe, or write pandas dataframe as csv"""

        if mode == 'r':
            self.data = pd.read_csv(self.fullpath)

        elif mode == 'w':
            data.to_csv(self.fullpath, index=False)
            self.logger.info(f'Pandas data saved at {self.fullpath}')

    def pickle_handler(self, mode, data):
        """Either read pickle as pandas dataframe, or write pickle dataframe as csv"""

        if mode == 'r':
            self.data = pd.read_pickle(self.fullpath)

        elif mode == 'w':
            data.to_pickle(self.fullpath)
            self.logger.info(f'Pandas data saved at {self.fullpath}')

    def write_output(self, data, filename=None, filepath=None):

        opath = os.path.join(filepath, filename)

        opath = self.fullpath if filename is None else os.path.join(filepath, filename)

        with open(opath, 'w') as results_file:
            results_file.write(data)

    def read_input(self, filename=None, filepath=None):

        output = ''
        ipath = self.fullpath if filename is None else os.path.join(filepath, filename)

        with open(ipath, 'r') as results_file:
            for line in results_file:
                output += line

        return output