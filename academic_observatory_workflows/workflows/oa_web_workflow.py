# Copyright 2021 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: James Diprose


import json
import os
import os.path
import shutil
import urllib.parse
from typing import Optional, List, Dict, Tuple
from urllib.parse import urlparse

import google.cloud.bigquery as bigquery
import pandas as pd
import pendulum
from airflow.exceptions import AirflowException
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

from academic_observatory_workflows.clearbit import clearbit_download_logo
from observatory.platform.utils.airflow_utils import get_airflow_connection_password, AirflowVars
from observatory.platform.utils.gc_utils import (
    select_table_shard_dates,
    bigquery_sharded_table_id,
    download_blobs_from_cloud_storage,
)
from observatory.platform.utils.url_utils import get_url_domain_suffix
from observatory.platform.utils.workflow_utils import make_release_date
from observatory.platform.workflows.snapshot_telescope import SnapshotRelease
from observatory.platform.workflows.workflow import Workflow

# The minimum number of outputs before including an entity in the analysis
INCLUSION_THRESHOLD = 1000

# The query that pulls data to be included in the dashboards
QUERY = """
SELECT
  agg.id,
  agg.name,
  agg.time_period as year,
  (SELECT * from grid.links LIMIT 1) AS url,
  (SELECT * from grid.types LIMIT 1) AS type,
  DATE(agg.time_period, 12, 31) as date,
  agg.total_outputs as n_outputs,
  agg.access_types.oa.total_outputs AS n_outputs_oa,
  agg.access_types.gold.total_outputs AS n_outputs_gold,
  agg.access_types.green.total_outputs AS n_outputs_green,
  agg.access_types.hybrid.total_outputs AS n_outputs_hybrid,
  agg.access_types.bronze.total_outputs AS n_outputs_bronze
FROM
  `{project_id}.{agg_dataset_id}.{agg_table_id}` as agg 
  LEFT OUTER JOIN `{project_id}.{grid_dataset_id}.{grid_table_id}` as grid ON agg.id = grid.id
WHERE agg.time_period <= EXTRACT(YEAR FROM CURRENT_DATE())
ORDER BY year DESC, name ASC
"""

# Overrides for country names
NAME_OVERRIDES = {
    "Bolivia (Plurinational State of)": "Bolivia",
    "Bosnia and Herzegovina": "Bosnia",
    "Brunei Darussalam": "Brunei",
    "Congo": "Congo Republic",
    "Congo, Democratic Republic of the": "DR Congo",
    "Iran (Islamic Republic of)": "Iran",
    "Korea (Democratic People's Republic of)": "North Korea",
    "Korea, Republic of": "South Korea",
    "Lao People's Democratic Republic": "Laos",
    "Micronesia (Federated States of)": "Micronesia",
    "Moldova, Republic of": "Moldova",
    "Palestine, State of": "Palestine",
    "Saint Kitts and Nevis": "St Kitts & Nevis",
    "Saint Lucia": "St Lucia",
    "Saint Vincent and the Grenadines": "St Vincent",
    "Sint Maarten (Dutch part)": "Sint Maarten",
    "Svalbard and Jan Mayen": "Svalbard & Jan Mayen",
    "Syrian Arab Republic": "Syria",
    "Taiwan, Province of China": "Taiwan",
    "Tanzania, United Republic of": "Tanzania",
    "Trinidad and Tobago": "Trinidad & Tobago",
    "United Kingdom of Great Britain and Northern Ireland": "United Kingdom",
    "United States of America": "United States",
    "Venezuela (Bolivarian Republic of)": "Venezuela",
    "Viet Nam": "Vietnam",
    "Virgin Islands (British)": "Virgin Islands",
    "Antigua and Barbuda": "Antigua & Barbuda",
    "Russian Federation": "Russia",
}


def bq_query_to_gcs(*, query: str, project_id: str, destination_uri: str, location: str = "us") -> bool:
    """Run a BigQuery query and save the results on Google Cloud Storage.

    :param query: the query string.
    :param project_id: the Google Cloud project id.
    :param destination_uri: the Google Cloud Storage destination uri.
    :param location: the BigQuery dataset location.
    :return: the status of the job.
    """

    client = bigquery.Client()

    # Run query
    query_job: bigquery.QueryJob = client.query(query, location=location)
    query_job.result()

    # Create and run extraction job
    source_table_id = f"{project_id}.{query_job.destination.dataset_id}.{query_job.destination.table_id}"
    extract_job_config = bigquery.ExtractJobConfig()
    extract_job_config.destination_format = bigquery.DestinationFormat.CSV
    extract_job: bigquery.ExtractJob = client.extract_table(
        source_table_id, destination_uri, job_config=extract_job_config, location=location
    )
    extract_job.result()

    return query_job.state == "DONE" and extract_job.state == "DONE"


def clean_url(url: str) -> str:
    """Remove path and query from URL.

    :param url: the url.
    :return: the cleaned url.
    """

    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}/"


class OaWebRelease(SnapshotRelease):
    PERCENTAGE_FIELD_KEYS = ["outputs_oa", "outputs_gold", "outputs_green", "outputs_hybrid", "outputs_bronze"]

    def __init__(
        self,
        *,
        dag_id: str,
        project_id: str,
        release_date: pendulum.DateTime,
        change_chart_years: int = 10,
        agg_dataset_id: str = "observatory",
        grid_dataset_id: str = "digital_science",
    ):
        """Create an OaWebRelease instance.

        :param dag_id: the dag id.
        :param project_id: the Google Cloud project id.
        :param release_date: the release date.
        :param change_chart_years: the number of years to include in the change charts.
        :param agg_dataset_id: the dataset to use for aggregation.
        :param grid_dataset_id: the GRID dataset id.
        """

        super().__init__(dag_id=dag_id, release_date=release_date)
        self.project_id = project_id
        self.change_chart_years = change_chart_years
        self.agg_dataset_id = agg_dataset_id
        self.grid_dataset_id = grid_dataset_id

    @property
    def build_path(self):
        return os.path.join(self.transform_folder, "build")

    def load_csv(self, category: str) -> pd.DataFrame:
        """Load the CSV file for a given category.

        :param category: the category, i.e. country or institution.
        :return: the Pandas Dataframe.
        """

        # Load CSV
        csv_path = os.path.join(self.download_folder, f"{category}.csv")
        df = pd.read_csv(csv_path)
        df["date"] = pd.to_datetime(df["date"])
        df.fillna("", inplace=True)

        return df

    def make_index(self, df: pd.DataFrame, category: str):
        """Make the data for the index tables.

        :param df: Pandas dataframe with all data points.
        :param category: the category, i.e. country or institution.
        :return:
        """

        # Aggregate
        df_index_table = df.groupby(["id"]).agg(
            {
                "id": "first",
                "name": "first",
                "url": "first",
                "type": "first",
                "n_outputs": "sum",
                "n_outputs_oa": "sum",
                "n_outputs_gold": "sum",
                "n_outputs_green": "sum",
                "n_outputs_hybrid": "sum",
                "n_outputs_bronze": "sum",
            },
            index=False,
        )

        # Exclude countries with small samples
        df_index_table = df_index_table[df_index_table["n_outputs"] >= INCLUSION_THRESHOLD]

        # Add percentages to dataframe
        self.update_df_with_percentages(df_index_table, self.PERCENTAGE_FIELD_KEYS)

        # Sort from highest oa percentage to lowest
        df_index_table.sort_values(by=["p_outputs_oa"], ascending=False, inplace=True)

        # Add ranks
        df_index_table["rank"] = list(range(1, len(df_index_table) + 1))

        # Add category
        df_index_table["category"] = category

        # Name overrides
        df_index_table["name"] = df_index_table["name"].apply(
            lambda name: NAME_OVERRIDES[name] if name in NAME_OVERRIDES else name
        )

        # Clean URLs
        df_index_table["friendly_url"] = df_index_table["url"].apply(
            lambda u: get_url_domain_suffix(u) if not pd.isnull(u) else u
        )

        # If country add wikipedia url
        if category == "country":
            df_index_table["url"] = df_index_table["name"].apply(
                lambda name: f"https://en.wikipedia.org/wiki/{urllib.parse.quote(name)}"
            )

        return df_index_table

    def update_df_with_percentages(self, df: pd.DataFrame, keys: List[str]):
        """Calculate percentages for fields in a Pandas dataframe.

        :param df: the Pandas dataframe.
        :param keys: they keys to calculate percentages for.
        :return: None.
        """

        for key in keys:
            df[f"p_{key}"] = round(df[f"n_{key}"] / df["n_outputs"] * 100, 0)

    def update_index_with_logos(self, df_index_table: pd.DataFrame, category: str, size=32, fmt="jpg"):
        """Update the index with logos, downloading logos if the don't exist.

        :param df_index_table: the index table Pandas dataframe.
        :param category: the category, i.e. country or institution.
        :param size: the image size.
        :param fmt: the image format.
        :return: None.
        """

        # Make logos
        if category == "country":
            df_index_table["logo"] = df_index_table["id"].apply(
                lambda country_code: f"/logos/{category}/{country_code}.svg"
            )
        elif category == "institution":
            base_path = os.path.join(self.build_path, "logos", category)
            logo_path_unknown = f"/unknown.svg"
            os.makedirs(base_path, exist_ok=True)
            logos = []
            for i, row in df_index_table.iterrows():
                grid_id = row["id"]
                url = clean_url(row["url"])

                logo_path = logo_path_unknown
                if not pd.isnull(url):
                    file_path = os.path.join(base_path, f"{grid_id}.{fmt}")
                    if not os.path.isfile(file_path):
                        clearbit_download_logo(company_url=url, file_path=file_path, size=size, fmt=fmt)

                    if os.path.isfile(file_path):
                        logo_path = f"/logos/{category}/{grid_id}.{fmt}"

                logos.append(logo_path)
            df_index_table["logo"] = logos

    def save_index(self, df_index_table: pd.DataFrame, category: str):
        """Save the index table.

        :param df_index_table: the index table Pandas Dataframe.
        :param category: the category, i.e. country or institution.
        :return: None.
        """

        # Save subset
        base_path = os.path.join(self.build_path, "data", category)
        os.makedirs(base_path, exist_ok=True)
        summary_path = os.path.join(base_path, "summary.json")
        columns = [
            "id",
            "rank",
            "name",
            "category",
            "logo",
            "p_outputs_oa",
            "p_outputs_gold",
            "p_outputs_green",
            "n_outputs",
            "n_outputs_oa",
        ]
        df_subset = df_index_table[columns]
        df_subset.to_json(summary_path, orient="records")

    def save_entity_details(self, df_index_table: pd.DataFrame, category: str):
        """Save the summary data for each entity, saving the result for each entity as a JSON file.

        :param df_index_table: a Pandas dataframe.
        :param category: the category, i.e. country or institution.
        :return: None.
        """

        base_path = os.path.join(self.build_path, "data", category)
        os.makedirs(base_path, exist_ok=True)
        df_index_table["category"] = category
        records = df_index_table.to_dict("records")
        for row in records:
            entity_id = row["id"]
            output_path = os.path.join(base_path, f"{entity_id}_summary.json")
            with open(output_path, mode="w") as f:
                json.dump(row, f, separators=(",", ":"))

    def make_timeseries(self, df: pd.DataFrame) -> List[Tuple[str, pd.DataFrame]]:
        """Make timeseries data for each entity, returning them.

        :param df: the Pandas dataframe containing all data points for a particular category.
        :return: a dictionary with keys for each entity and change points data for each value.
        """

        results = []
        ts_groups = df.groupby(["id"])
        for entity_id, df_group in ts_groups:
            # Exclude institutions with small num outputs
            total_outputs = df_group["n_outputs"].sum()
            if total_outputs >= INCLUSION_THRESHOLD:
                self.update_df_with_percentages(df_group, self.PERCENTAGE_FIELD_KEYS)
                df_group = df_group.sort_values(by=["year"])
                df_group = df_group.loc[:, ~df_group.columns.str.contains("^Unnamed")]

                # Save to csv
                df_ts: pd.DataFrame = df_group[
                    [
                        "year",
                        "n_outputs",
                        "n_outputs_oa",
                        "p_outputs_oa",
                        "p_outputs_gold",
                        "p_outputs_green",
                        "p_outputs_hybrid",
                        "p_outputs_bronze",
                    ]
                ]

                results.append((entity_id, df_ts))

        return results

    def save_timeseries(self, timeseries: List[Tuple[str, pd.DataFrame]], category: str):
        """Save the timeseries data.

        :param timeseries: the timeseries data for each entity. A list of tuples, with (entity id, entity dataframe).
        :param category: the category, i.e. country or institution.
        :return: None.
        """

        base_path = os.path.join(self.build_path, "data", category)
        os.makedirs(base_path, exist_ok=True)

        for entity_id, df_ts in timeseries:
            ts_path = os.path.join(base_path, f"{entity_id}_ts.json")
            df_ts.to_json(ts_path, orient="records")

    def make_auto_complete(self, df_index_table: pd.DataFrame, category: str):
        """Build the autocomplete data.

        :param df_index_table: index table Pandas dataframe.
        :param category: the category, i.e. country or institution.
        :return: autocomplete records.
        """

        records = []
        for i, row in df_index_table.iterrows():
            id = row["id"]
            name = row["name"]
            logo = row["logo"]
            records.append({"id": id, "name": name, "category": category, "logo": logo})
        return records

    def save_autocomplete(self, auto_complete: List[Dict]):
        """Save the autocomplete data.

        :param auto_complete: the autocomplete list.
        :return: None.
        """

        base_path = os.path.join(self.build_path, "data")
        os.makedirs(base_path, exist_ok=True)

        output_path = os.path.join(base_path, "autocomplete.json")
        df_ac = pd.DataFrame(auto_complete)
        df_ac.to_json(output_path, orient="records")


class OaWebWorkflow(Workflow):
    TASK_ID_BUILD_WEBSITE = "build_website"
    TASK_ID_DEPLOY_WEBSITE = "deploy_website"
    AIRFLOW_VAR_WEBSITE_FOLDER = "website_folder"
    AIRFLOW_CONN_CLOUDFLARE_API_TOKEN = "cloudflare_api_token"
    DEPLOY_WEBSITE_PATH = "/home/airflow/.local/bin:/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/home/airflow/.yarn/bin"

    def __init__(
        self,
        *,
        dag_id: str = "oa_web_workflow",
        start_date: Optional[pendulum.DateTime] = pendulum.datetime(2021, 5, 2),
        schedule_interval: Optional[str] = "@weekly",
        catchup: Optional[bool] = False,
        ext_dag_id: str = "doi",
        table_ids: List[str] = None,
        airflow_vars: List[str] = None,
        retries: int = 3,
        agg_dataset_id: str = "observatory",
        grid_dataset_id: str = "digital_science",
    ):
        """Create the OaWebWorkflow.

        :param dag_id: the DAG id.
        :param start_date: the start date.
        :param schedule_interval: the schedule interval.
        :param catchup: whether to catchup or not.
        :param table_ids: the table ids.
        :param airflow_vars: required Airflow Variables.
        """

        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=catchup,
            airflow_vars=airflow_vars,
        )
        self.agg_dataset_id = agg_dataset_id
        self.grid_dataset_id = grid_dataset_id
        self.table_ids = table_ids
        if table_ids is None:
            self.table_ids = ["country", "institution"]

        self.add_operator(
            ExternalTaskSensor(task_id=f"{ext_dag_id}_sensor", external_dag_id=ext_dag_id, mode="reschedule")
        )
        self.add_setup_task(self.check_dependencies)
        self.add_task(self.query)
        self.add_task(self.download)
        self.add_task(self.transform)
        # TODO: add in a task to clone a certain version of the website, when this is ready
        self.add_task(self.copy_static_assets)
        self.add_operator(
            BashOperator(
                task_id=self.TASK_ID_BUILD_WEBSITE,
                params={"website_folder": self.website_folder},
                bash_command="cd {{ params.website_folder }} && ./build.sh ",
                retries=retries,
            )
        )
        self.add_operator(
            BashOperator(
                task_id=self.TASK_ID_DEPLOY_WEBSITE,
                params={"website_folder": self.website_folder},
                env={
                    "CF_API_TOKEN": get_airflow_connection_password(self.AIRFLOW_CONN_CLOUDFLARE_API_TOKEN),
                    "PATH": self.DEPLOY_WEBSITE_PATH,
                },
                bash_command="cd {{ params.website_folder }} && ./deploy.sh ",
                retries=retries,
            )
        )

    @property
    def website_folder(self) -> str:
        """Get the path to the oa website folder.

        :return: the path to the oa website folder.
        """

        return Variable.get(self.AIRFLOW_VAR_WEBSITE_FOLDER)

    def make_release(self, **kwargs) -> OaWebRelease:
        """Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: A list of grid release instances
        """

        project_id = Variable.get(AirflowVars.PROJECT_ID)
        release_date = make_release_date(**kwargs)

        return OaWebRelease(
            dag_id=self.dag_id,
            project_id=project_id,
            release_date=release_date,
            grid_dataset_id=self.grid_dataset_id,
            agg_dataset_id=self.agg_dataset_id,
        )

    def query(self, release: OaWebRelease, **kwargs):
        """Fetch the data for each table.

        :param release: the release.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        results = []
        for agg_table_id in self.table_ids:
            # Aggregate release dates
            agg_release_date = select_table_shard_dates(
                project_id=release.project_id,
                dataset_id=release.agg_dataset_id,
                table_id=agg_table_id,
                end_date=release.release_date,
            )[0]
            agg_sharded_table_id = bigquery_sharded_table_id(agg_table_id, agg_release_date)

            # GRID release date
            grid_table_id = "grid"
            grid_release_date = select_table_shard_dates(
                project_id=release.project_id,
                dataset_id=release.grid_dataset_id,
                table_id=grid_table_id,
                end_date=release.release_date,
            )[0]
            grid_sharded_table_id = bigquery_sharded_table_id(grid_table_id, grid_release_date)

            # Fetch data
            destination_uri = f"gs://{release.download_bucket}/{self.dag_id}/{release.release_id}/{agg_table_id}.csv"
            success = bq_query_to_gcs(
                query=QUERY.format(
                    project_id=release.project_id,
                    agg_dataset_id=release.agg_dataset_id,
                    agg_table_id=agg_sharded_table_id,
                    grid_dataset_id=release.grid_dataset_id,
                    grid_table_id=grid_sharded_table_id,
                ),
                project_id=release.project_id,
                destination_uri=destination_uri,
            )
            results.append(success)

        state = all(results)
        if not state:
            raise AirflowException("OaWebWorkflow.query failed")

    def download(self, release: OaWebRelease, **kwargs):
        """Download the queried data.

        :param release: the release.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        prefix = f"{self.dag_id}/{release.release_id}"
        state = download_blobs_from_cloud_storage(
            bucket_name=release.download_bucket, prefix=prefix, destination_path=release.download_folder
        )
        if not state:
            raise AirflowException("OaWebWorkflow.download failed")

    def transform(self, release: OaWebRelease, **kwargs):
        """Transform the queried data into the final format for the open access website.

        :param release: the release.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        # Make required folders
        auto_complete = []
        for category in self.table_ids:
            # Load data
            df = release.load_csv(category)

            # Make index table
            df_index_table = release.make_index(df, category)
            release.update_index_with_logos(df_index_table, category)

            # Make autocomplete data for this category
            auto_complete += release.make_auto_complete(df_index_table, category)

            # Make timeseries
            entity_timeseries = release.make_timeseries(df)

            # Save category data
            release.save_index(df_index_table, category)
            release.save_entity_details(df_index_table, category)
            release.save_timeseries(entity_timeseries, category)

        # Save auto complete data as json
        release.save_autocomplete(auto_complete)

    def copy_static_assets(self, release: OaWebRelease, **kwargs):
        """Remove previously generated static assets from website and copy newly generated assets.

        :return: None.
        """

        # Remove existing build folder
        website_build_folder = os.path.join(self.website_folder, "static", "build")
        if os.path.exists(website_build_folder):
            shutil.rmtree(website_build_folder, ignore_errors=True)

        # Copy generated files to new build folder
        shutil.copytree(release.build_path, website_build_folder)
