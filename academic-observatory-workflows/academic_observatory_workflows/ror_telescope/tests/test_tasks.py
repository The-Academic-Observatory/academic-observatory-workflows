import os
import unittest

import pendulum
import vcr

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.ror_telescope.tasks import list_ror_records, is_lat_lng_valid, transform_ror

FIXTURES_FOLDER = project_path("ror_telescope", "tests", "fixtures")


class TestListRorRecords(unittest.TestCase):
    ror_conceptrecid = 6347574

    def test_list_ror_records(self):
        """Test that list_ror_records returns correct records"""

        # There are a number of releases near this range
        # 2023-04-12, 2023-03-30, 2023-03-16, 2023-02-28
        # It should just return 2023-03-30, 2023-03-16 as end_date is exclusive
        start_date = pendulum.datetime(2023, 3, 16)
        end_date = pendulum.datetime(2023, 4, 12)

        with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "list_ror_records.yaml")):
            records = list_ror_records(self.ror_conceptrecid, start_date, end_date)
            self.assertEqual(2, len(records))
            self.assertEqual(
                [
                    {
                        "snapshot_date": "20230330",
                        "url": "https://zenodo.org/api/records/7787102/files/v1.22-2023-03-30-ror-data.zip/content",
                        "checksum": "md5:019db485a7a88293851e91a0c4aa4206",
                    },
                    {
                        "snapshot_date": "20230316",
                        "url": "https://zenodo.org/api/records/7742581/files/v1.21-2023-03-16-ror-data.zip/content",
                        "checksum": "md5:2f24c6cf13186b4688ffa0f27abf2b5b",
                    },
                ],
                records,
            )


class TestIsLatLngValid(unittest.TestCase):
    def test_is_lat_lng_valid(self):
        """Test that lat lng valid"""

        self.assertTrue(is_lat_lng_valid(-90, -180))
        self.assertTrue(is_lat_lng_valid(90, 180))
        self.assertFalse(is_lat_lng_valid(90.1, 180.1))
        self.assertFalse(is_lat_lng_valid(-90.1, -180.1))


class TestTransformRor(unittest.TestCase):
    def test_transform_ror(self):
        """Test that ROR transforms, i.e. invalid data is removed"""

        records = [
            {
                "id": "a",
                "addresses": [
                    {
                        "lat": 48.854692,
                        "lng": 2.33781,
                    }
                ],
            },
            {
                "id": "b",
                "addresses": [
                    {
                        "lat": 48.854692,
                        "lng": 233781,
                    }
                ],
            },
        ]
        records = transform_ror(records)
        self.assertEqual(
            records,
            [
                {
                    "id": "a",
                    "addresses": [
                        {
                            "lat": 48.854692,
                            "lng": 2.33781,
                        }
                    ],
                },
                {
                    "id": "b",
                    "addresses": [
                        {
                            "lat": None,
                            "lng": None,
                        }
                    ],
                },
            ],
        )
