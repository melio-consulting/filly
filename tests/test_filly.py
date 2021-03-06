#!/usr/bin/env python

"""Tests for `filly` package."""

import os
import sys
import pytest
from contextlib import nullcontext
import pandas as pd
from pandas._testing import assert_frame_equal
# from click.testing import CliRunner

from filly.filly import Filly, FileUploadError
from filly import cli


##TODO: write CLI runner
# def test_command_line_interface():
#     """Test the CLI."""
#     runner = CliRunner()
#     result = runner.invoke(cli.main)
#     assert result.exit_code == 0
#     assert 'filly.cli.main' in result.output
#     help_result = runner.invoke(cli.main, ['--help'])
#     assert help_result.exit_code == 0
#     assert '--help  Show this message and exit.' in help_result.output

## TODO: mock S3

@pytest.fixture
def df():
    return pd.read_csv('tests/data/test.csv')


@pytest.mark.parametrize(
    "filename, filepath, to_raise, expected_raises",
    [
        ('valid_name', 'valid_path', False, nullcontext()),
        (
            'valid_name', 'valid_path', True,
            pytest.raises(
                FileUploadError,
                match='Upload failed for File valid_name upload to valid_path'
            )
        )
    ]
)
def test_file_upload_error(filename, filepath, to_raise, expected_raises):

    if to_raise:
        with expected_raises:
            raise FileUploadError(filename, filepath)

@pytest.mark.parametrize(
    "remote, bucket_name", [
        ('s3', None),
        ('s3', '')
    ]
)
def test_remote(remote, bucket_name):
    with pytest.raises(ValueError):
        Filly(
            remote=remote,
            bucket_name=bucket_name
        )

@pytest.mark.parametrize(
    "filename, filepath, fullpath, mode",
    [
        ('test_csv.csv', 'tests/data/', None, 'w'),
        ('test_csv_read.csv', 'tests/data/', None, 'r'),
        (None, None, 'tests/data/test_csv_read.csv', 'r')
    ]
)
def test_csv_handler(filename, filepath, fullpath, mode):

    dict1 = pd.DataFrame({
        "A": [0,0,0],
        "B": [1,1,1],
        "C": [2,2,2]
    })

    if mode == 'w':
        try:
            file_handler = Filly()
            file_handler.write_data(
                filename=filename,
                filepath=filepath,
                fullpath=fullpath,
                data=dict1
            )
            dict2 = pd.read_csv(open(os.path.join(filepath, filename), 'r'))
            assert_frame_equal(dict1, dict2)
        except Exception as err:
            print(err)
        finally:
            os.remove(file_handler.fullpath)

    elif mode == 'r':

        file_handler = Filly(remote=None)
        file_handler.read_data(filename=filename, filepath=filepath, fullpath=fullpath)
        assert_frame_equal(file_handler.data, dict1)

@pytest.mark.parametrize(
    "filename, filepath, fullpath, mode",
    [
        ('test_pickle.pkl', 'tests/data/', None,'w'),
        ('test_pickle_read.pkl', 'tests/data/', None, 'r'),
        (None, None, 'tests/data/test_pickle.pkl', 'w'),
        (None, None, 'tests/data/test_pickle_read.pkl', 'r')
    ]
)
def test_pickle_handler(filename, filepath, fullpath, mode):

    dict1 = pd.DataFrame({
        "A": [0,0,0],
        "B": [1,1,1],
        "C": [2,2,2]
    })

    if mode == 'w':
        try:
            file_handler = Filly()
            file_handler.write_data(
                filename=filename,
                filepath=filepath,
                fullpath=fullpath,
                data=dict1
            )
            dict2 = pd.read_csv(open(os.path.join(filepath, filename), 'r'))
            assert_frame_equal(dict1, dict2)
        except Exception as err:
            print(err)
        finally:
            os.remove(file_handler.fullpath)

    elif mode == 'r':

        file_handler = Filly(remote=None)
        file_handler.read_data(filename=filename, filepath=filepath, fullpath=fullpath)
        assert_frame_equal(file_handler.data, dict1)

@pytest.mark.parametrize(
    "filename, filepath, mode",
    [
        ('test_json.json', 'tests/data/', 'w'),
        ('test_json_read.json', 'tests/data/', 'r')
    ]
)
def test_json_handler(filename, filepath, mode):

    dict1 = {"A": 1, "B": 2, "C": 3}

    if mode == 'w':
        try:
            file_handler = Filly()
            file_handler.write_data(
                filename=filename,
                filepath=filepath,
                data=dict1
            )
            dict2 = json.loads(open(os.path.join(filepath, filename), 'r').read())
            assert dict1 == dict2
        except Exception as err:
            print(err)
        finally:
            os.remove(file_handler.fullpath)

    elif mode == 'r':

        file_handler = Filly(remote=None)
        file_handler.read_data(filename=filename, filepath=filepath)
        assert file_handler.data == dict1


@pytest.mark.parametrize(
    "filename, filepath, fullpath, data",
    [
        ('tmp', 'tests/data/', 'tests/data/tmp', 'true'),
        (None, None, 'tests/data/tmp', 'true')
    ]
)
def test_write_output(filename, filepath, fullpath, data):

    file_handler = Filly()
    file_handler.write_output(data=data, filepath=filepath, filename=filename, fullpath=fullpath)

    output = ''
    with open(fullpath, 'r') as results_file:
        for line in results_file:
            output += line

    assert output == data


def test_read_input():

    tmp = Filly().read_input(filepath='tests/data', filename='tmp')

    assert tmp == 'true'

def test_read_data():

    dict1 = pd.DataFrame({
        "A": [0,0,0],
        "B": [1,1,1],
        "C": [2,2,2]
    })

    filly = Filly(remote='s3', bucket_name='tmp')
    filly.read_data(filename='test_csv_read.csv', filepath='tests/data', download=False)
    assert_frame_equal(filly.data, dict1)
