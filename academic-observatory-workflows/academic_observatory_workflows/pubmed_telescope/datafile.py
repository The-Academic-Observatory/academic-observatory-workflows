from __future__ import annotations

import os
from typing import Dict

import pendulum

from observatory_platform.airflow.release import ChangefileRelease as DatafileRelease


class Datafile:
    def __init__(
        self,
        filename: str,
        file_index: int,
        baseline: bool,
        path_on_ftp: str,
        datafile_date: pendulum.DateTime,
        datafile_release: DatafileRelease = None,
    ):
        """Holds the metadata about a single Pubmed datafile.

        Pubmed is organised into a yearly "baseline" snapshots and then "updatefiles" are released daily afterwards
        to modify and or add to the snapshot table. To avoid confusion, we call both the baseline and updatefiles a "datafile".
        Each datafile could hold an upsert or delete record and are extracted in later parts of the workflow.

        :param filename: the name of the datafile.
        :param file_index: Index of the datafile_date, effectively a count of the n-th datafile.
        :param baseline: Boolean for if this is from the bsaeline set of files. False is it is from the updatefiles set.
        :param path_on_ftp: Path of the file on Pubmed's FTP server.
        :param datafile_date: The date that the datafile_date was added to PubMed's FTP server.
        :param datafile_release: The Datafile release, helps give download and transform paths to files.
        """

        self.filename = filename
        self.file_index = file_index
        self.baseline = baseline
        self.path_on_ftp = path_on_ftp
        self.datafile_date = datafile_date
        self.datafile_release: DatafileRelease = datafile_release
        self.file_type = "jsonl.gz"

    def __eq__(self, other):
        if isinstance(other, Datafile):
            return (
                self.filename == other.filename
                and self.file_index == other.file_index
                and self.baseline == other.baseline
                and self.path_on_ftp == other.path_on_ftp
                and self.datafile_date == other.datafile_date
            )
        return False

    def from_dict(dict_: Dict) -> Datafile:
        filename = dict_["filename"]
        file_index = dict_["file_index"]
        baseline = dict_["baseline"]
        path_on_ftp = dict_["path_on_ftp"]
        datafile_date = pendulum.parse(dict_["datafile_date"])

        return Datafile(
            filename=filename,
            file_index=file_index,
            baseline=baseline,
            path_on_ftp=path_on_ftp,
            datafile_date=datafile_date,
        )

    def to_dict(self) -> Dict:
        return dict(
            filename=self.filename,
            file_index=self.file_index,
            baseline=self.baseline,
            path_on_ftp=self.path_on_ftp,
            datafile_date=self.datafile_date.isoformat(),
        )

    @property
    def download_file_path(self):
        assert self.datafile_release is not None, "Datafile.download_folder: self.datafile_release is None"
        return os.path.join(self.datafile_release.download_folder, self.filename)

    @property
    def transform_baseline_file_path(self):
        assert self.datafile_release is not None, "Datafile.transform_baseline_path: self.datafile_release is None"
        return os.path.join(self.datafile_release.transform_folder, f"baseline_{self.filename[:-7]}.jsonl.gz")

    @property
    def transform_upsert_file_path(self):
        assert self.datafile_release is not None, "Datafile.transform_upsert_path: self.datafile_release is None"
        return os.path.join(self.datafile_release.transform_folder, f"upserts_{self.filename[:-7]}.jsonl")

    @property
    def transform_delete_file_path(self):
        assert self.datafile_release is not None, "Datafile.delete_file_path: self.datafile_release is None"
        return os.path.join(self.datafile_release.transform_folder, f"deletes_{self.filename[:-7]}.jsonl")

    @property
    def merged_upsert_file_path(self):
        assert self.datafile_release is not None, "Datafile.merged_upsert_path: self.datafile_release is None"
        return os.path.join(self.datafile_release.transform_folder, f"upsert_merged_{self.filename[:-7]}.jsonl.gz")
