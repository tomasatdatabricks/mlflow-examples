# pylint: disable=redefined-outer-name
from mock import Mock, MagicMock
import pytest
import os
import ftplib
from ftplib import FTP

from mlflow.store.artifact_repo import ArtifactRepository
from mlflow.store.ftp_artifact_repo import FTPArtifactRepository


@pytest.fixture
def ftp_mock():
    return MagicMock(autospec=FTP)


def test_artifact_uri_factory():
    repo = ArtifactRepository.from_artifact_uri("ftp://user:pass@test_ftp:123/some/path", Mock())
    assert isinstance(repo, FTPArtifactRepository)


def test_list_artifacts_empty(ftp_mock):
    repo = FTPArtifactRepository("ftp://test_ftp/some/path")

    repo.get_ftp_client = MagicMock()
    call_mock = MagicMock(return_value=ftp_mock)
    repo.get_ftp_client.return_value = MagicMock(__enter__=call_mock)

    ftp_mock.nlst = MagicMock(return_value=[])
    assert repo.list_artifacts() == []
    ftp_mock.nlst.assert_called_once_with("/some/path")


def test_list_artifacts(ftp_mock):
    artifact_root_path = "/experiment_id/run_id/"
    repo = FTPArtifactRepository("ftp://test_ftp"+artifact_root_path)

    repo.get_ftp_client = MagicMock()
    call_mock = MagicMock(return_value=ftp_mock)
    repo.get_ftp_client.return_value = MagicMock(__enter__=call_mock)

    # mocked file structure
    #  |- file
    #  |- model
    #     |- model.pb

    file_path = "file"
    file_size = 678
    dir_path = "model"
    ftp_mock.cwd = MagicMock(side_effect=[None, ftplib.error_perm, None])
    ftp_mock.nlst = MagicMock(return_value=[file_path, dir_path])

    ftp_mock.size = MagicMock(return_value=file_size)

    artifacts = repo.list_artifacts(path=None)

    ftp_mock.nlst.assert_called_once_with(artifact_root_path)
    ftp_mock.size.assert_called_once_with(artifact_root_path + file_path)

    assert len(artifacts) == 2
    assert artifacts[0].path == file_path
    assert artifacts[0].is_dir is False
    assert artifacts[0].file_size == file_size
    assert artifacts[1].path == dir_path
    assert artifacts[1].is_dir is True
    assert artifacts[1].file_size is None


def test_list_artifacts_with_subdir(ftp_mock):
    artifact_root_path = "/experiment_id/run_id/"
    repo = FTPArtifactRepository("sftp://test_sftp"+artifact_root_path)

    repo.get_ftp_client = MagicMock()
    call_mock = MagicMock(return_value=ftp_mock)
    repo.get_ftp_client.return_value = MagicMock(__enter__=call_mock)

    # mocked file structure
    #  |- model
    #     |- model.pb
    #     |- variables
    dir_name = 'model'

    # list artifacts at sub directory level
    file_path = 'model.pb'
    file_size = 345
    subdir_name = 'variables'

    ftp_mock.nlst = MagicMock(return_value=[file_path, subdir_name])

    ftp_mock.cwd = MagicMock(side_effect=[None, ftplib.error_perm, None])

    ftp_mock.size = MagicMock(return_value=file_size)

    artifacts = repo.list_artifacts(path=dir_name)

    ftp_mock.nlst.assert_called_once_with(artifact_root_path + dir_name)
    ftp_mock.size.assert_called_once_with(artifact_root_path + dir_name + '/' + file_path)

    assert len(artifacts) == 2
    assert artifacts[0].path == dir_name + '/' + file_path
    assert artifacts[0].is_dir is False
    assert artifacts[0].file_size == file_size
    assert artifacts[1].path == dir_name + '/' + subdir_name
    assert artifacts[1].is_dir is True
    assert artifacts[1].file_size is None


def test_log_artifact(ftp_mock, tmpdir):
    repo = FTPArtifactRepository("ftp://test_ftp/some/path")

    repo.get_ftp_client = MagicMock()
    call_mock = MagicMock(return_value=ftp_mock)
    repo.get_ftp_client.return_value = MagicMock(__enter__=call_mock)

    d = tmpdir.mkdir("data")
    f = d.join("test.txt")
    f.write("hello world!")
    fpath = d + '/test.txt'
    fpath = fpath.strpath

    repo.log_artifact(fpath)

    ftp_mock.mkd.assert_called_once_with('/some/path')
    ftp_mock.cwd.assert_called_once_with('/some/path')
    ftp_mock.storbinary.assert_called_once()
    assert ftp_mock.storbinary.call_args_list[0][0][0] == 'STOR test.txt'


def test_log_artifacts(ftp_mock, tmpdir):
    repo = FTPArtifactRepository("ftp://test_ftp/some/path")

    repo.get_ftp_client = MagicMock()
    call_mock = MagicMock(return_value=ftp_mock)
    repo.get_ftp_client.return_value = MagicMock(__enter__=call_mock)

    subd = tmpdir.mkdir("data").mkdir("subdir")
    subd.join("a.txt").write("A")
    subd.join("b.txt").write("B")
    subd.join("c.txt").write("C")

    repo.log_artifacts(subd.strpath)

    ftp_mock.mkd.assert_any_call('/some/path/subdir')
    ftp_mock.cwd.assert_any_call('/some/path/subdir')
    assert ftp_mock.storbinary.call_count == 3
    storbinary_call_args = sorted([ftp_mock.storbinary.call_args_list[i][0][0] for i in range(3)])
    assert storbinary_call_args == ['STOR a.txt', 'STOR b.txt', 'STOR c.txt']


def test_download_artifacts_single(ftp_mock):
    repo = FTPArtifactRepository("ftp://test_ftp/some/path")

    repo.get_ftp_client = MagicMock()
    call_mock = MagicMock(return_value=ftp_mock)
    repo.get_ftp_client.return_value = MagicMock(__enter__=call_mock)

    ftp_mock.cwd = MagicMock(side_effect=ftplib.error_perm)

    repo.download_artifacts("test.txt")

    ftp_mock.retrbinary.assert_called_once()
    assert ftp_mock.retrbinary.call_args_list[0][0][0] == 'RETR /some/path/test.txt'


def test_download_artifacts(ftp_mock):
    artifact_root_path = "/some/path"
    repo = FTPArtifactRepository("ftp://test_ftp" + artifact_root_path)

    repo.get_ftp_client = MagicMock()
    call_mock = MagicMock(return_value=ftp_mock)
    repo.get_ftp_client.return_value = MagicMock(__enter__=call_mock)

    # mocked file structure
    #  |- model
    #     |- model.pb
    #     |- variables
    #        |- test.txt
    dir_path = os.path.join(artifact_root_path, 'model')

    # list artifacts at sub directory level
    model_file_path_sub = 'model.pb'
    model_file_path_full = os.path.join(dir_path, model_file_path_sub)
    subdir_name = 'variables'
    subdir_path_full = os.path.join(dir_path, subdir_name)
    subfile_name = 'test.txt'
    subfile_path_full = os.path.join(artifact_root_path, subdir_path_full, subfile_name)

    is_dir_mapping = {
        dir_path: True,
        model_file_path_full: False,
        subdir_path_full: True,
        subfile_path_full: False,
    }

    is_dir_call_args = [
        dir_path, model_file_path_full, subdir_path_full,
        model_file_path_full,
        subdir_path_full, subfile_path_full,
        subfile_path_full
    ]

    cwd_side_effect = [
        None if is_dir_mapping[call_arg] else ftplib.error_perm for call_arg in is_dir_call_args
    ]
    ftp_mock.cwd = MagicMock(side_effect=cwd_side_effect)
    ftp_mock.nlst = MagicMock(side_effect=[[model_file_path_sub, subdir_name], [subfile_name]])

    repo.download_artifacts("model")

    cwd_call_args = [arg_entry[0][0] for arg_entry in ftp_mock.cwd.call_args_list]
    assert cwd_call_args == is_dir_call_args
    assert ftp_mock.nlst.call_count == 2
    assert ftp_mock.retrbinary.call_args_list[0][0][0] == 'RETR ' + model_file_path_full
    assert ftp_mock.retrbinary.call_args_list[1][0][0] == 'RETR ' + subfile_path_full
