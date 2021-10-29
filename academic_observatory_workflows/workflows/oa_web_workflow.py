# Copyright 2021 Curtin University
# Copyright 2021 Artificial Dimensions Limited
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
import urllib.parse
from typing import Optional, List, Dict

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


def draw_change_points(values: List[float], width=166, height=48, stroke_width=2) -> str:
    """Make the change points for the change charts.

    :param values: the list of values to plot.
    :param width: the width of the chart.
    :param height: the height of the chart.
    :param stroke_width: the stroke width.
    :return: the change points as strings.
    """

    # Calculate means and scale
    x_scale = width / (len(values) - 1)

    # Use the following formula to scale y:
    # https://math.stackexchange.com/questions/914823/shift-numbers-into-a-different-range
    a = max(values)
    b = min(values)
    c = stroke_width
    d = height - stroke_width

    def scale_y(t):
        return c + (d - c) / (b - a) * (t - a)

    # Draw lines
    points = []
    all_zero = all(v == 0 for v in values)
    for x, y in enumerate(values):
        sx = x * x_scale
        if not all_zero:
            sy = scale_y(y)
        else:
            sy = 0.0
        points.append(f"{sx},{sy}")

    return " ".join(points)


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


def calc_percentages(df: pd.DataFrame, keys: List[str]):
    """Calculate percentages for fields in a Pandas dataframe.

    :param df: the Pandas dataframe.
    :param keys: they keys to calculate percentages for.
    :return: None.
    """

    for key in keys:
        df[f"p_{key}"] = round(df[f"n_{key}"] / df.n_outputs * 100, 0)


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

    def make_index_table_data(self, df: pd.DataFrame, ts_data: Dict, category: str):
        """Make the data for the index tables.

        :param df: Pandas dataframe with all data points.
        :param ts_data: timeseries data.
        :param category: the category.
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
        df_index_table = df_index_table[df_index_table.n_outputs >= INCLUSION_THRESHOLD]

        # Add percentages to dataframe
        calc_percentages(df_index_table, self.PERCENTAGE_FIELD_KEYS)

        # Sort from highest oa percentage to lowest
        df_index_table.sort_values(by=["p_outputs_oa"], ascending=False, inplace=True)

        # Add ranks
        df_index_table.rank = list(range(1, len(df_index_table) + 1))

        # Add category
        df_index_table.category = category

        # Name overrides
        df_index_table.name = df_index_table.name.apply(
            lambda name: NAME_OVERRIDES[name] if name in NAME_OVERRIDES else name
        )

        # Clean URLs
        df_index_table.friendly_url = df_index_table.url.apply(
            lambda u: get_url_domain_suffix(u) if not pd.isnull(u) else u
        )

        # If country add wikipedia url
        if category == "country":
            df_index_table.url = df_index_table.name.apply(
                lambda name: f"https://en.wikipedia.org/wiki/{urllib.parse.quote(name)}"
            )

        # Integrate time series data
        change_points = []
        for i, row in df_index_table.iterrows():
            entity_id = row.id
            change_points.append(ts_data[entity_id])
        df_index_table.change_points = change_points

        # Make logos
        self.download_institution_logos(df_index_table, category)

        # Save subset
        base_path = os.path.join(self.transform_folder, "data", category)
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
            "change_points",
        ]
        df_summary_subset = df_index_table[columns]
        df_summary_subset.to_json(summary_path, orient="records")
        return df_index_table

    def save_entity_details_data(self, df: pd.DataFrame, category: str):
        """Save the summary data for each entity, saving the result for each entity as a JSON file.

        :param df: a Pandas dataframe.
        :param category: the entity category.
        :return: None.
        """

        base_path = os.path.join(self.transform_folder, "data", category)
        os.makedirs(base_path, exist_ok=True)
        df["category"] = category
        records = df.to_dict("records")
        for row in records:
            entity_id = row.id
            output_path = os.path.join(base_path, f"{entity_id}_summary.json")
            with open(output_path, mode="w") as f:
                json.dump(row, f, separators=(",", ":"))

    def download_institution_logos(self, df: pd.DataFrame, category: str, size=32, fmt="jpg"):
        """Download institution logos.

        :param df: the index table Pandas dataframe.
        :param category: the entity category.
        :param size: the image size.
        :param fmt: the image format.
        :return: None.
        """

        # Make logos
        if category == "country":
            df.logo = df.id.apply(lambda country_code: f"/logos/{category}/{country_code}.svg")
        elif category == "institution":
            base_path = os.path.join(self.transform_folder, "logos", category)
            logo_path_unknown = f"/unknown.svg"
            os.makedirs(base_path, exist_ok=True)
            logos = []
            for i, row in df.iterrows():
                grid_id = row["id"]
                url = row["url"]
                logo_path = logo_path_unknown
                if not pd.isnull(url):
                    file_path = os.path.join(base_path, f"{grid_id}.{fmt}")
                    if not os.path.isfile(file_path):
                        clearbit_download_logo(company_url=url, file_path=file_path, size=size, fmt=fmt)

                    if os.path.isfile(file_path):
                        logo_path = f"/logos/{category}/{grid_id}.{fmt}"

                logos.append(logo_path)
            df.logo = logos

    def make_timeseries_data(self, df: pd.DataFrame, category: str) -> Dict:
        """Make timeseries data for each entity.

        :param df: the Pandas dataframe containing all data points.
        :param category: the entity category.
        :return: a dictionary with keys for each entity and timeseries data for each value.
        """

        ts_data = {}

        # Time series statistics for each entity
        base_path = os.path.join(self.transform_folder, "data", category)
        os.makedirs(base_path, exist_ok=True)
        ts_groups = df.groupby(["id"])
        for entity_id, df_group in ts_groups:
            # Exclude institutions with small num outputs
            total_outputs = df_group.n_outputs.sum()
            if total_outputs >= INCLUSION_THRESHOLD:
                calc_percentages(df_group, self.PERCENTAGE_FIELD_KEYS)
                df_group = df_group.sort_values(by=["year"])
                df_group = df_group.loc[:, ~df_group.columns.str.contains("^Unnamed")]

                # Save to csv
                df_group = df_group[
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
                ts_path = os.path.join(base_path, f"{entity_id}_ts.json")
                df_group.to_json(ts_path, orient="records")

                # Fill in data for years with missing points
                df_ts = df_group[["year", "p_outputs_oa"]]
                end = pendulum.now().year
                start = end - self.change_chart_years - 1
                df_ts = df_ts[(start <= df_ts.year) & (df_ts.year < end)]  # Filter

                df_ts.set_index("year", inplace=True)
                df_ts = df_ts.reindex(list(range(start, end)))
                df_ts = df_ts.sort_values(by=["year"])
                all_null = df_ts.p_outputs_oa.isnull().values.any()
                if all_null:
                    df_ts.p_outputs_oa = df_ts.p_outputs_oa.fillna(0)
                else:
                    df_ts.interpolate(method="linear", inplace=True)

                ts_data[entity_id] = draw_change_points(df_ts.p_outputs_oa.tolist())

        return ts_data

    def make_auto_complete_data(self, df: pd.DataFrame, category: str):
        """Build the autocomplete data.

        :param df: index table Pandas dataframe.
        :param category: entity category.
        :return: autocomplete records.
        """

        records = []
        for i, row in df.iterrows():
            id = row.id
            name = row.name
            logo = row.logo
            records.append({"id": id, "name": name, "category": category, "logo": logo})
        return records


class OaWebWorkflow(Workflow):
    AIRFLOW_VAR_WEBSITE_FOLDER = "website_folder"
    AIRFLOW_CONN_CLOUDFLARE_API_TOKEN = "cloudflare_api_token"
    DEPLOY_WEBSITE_PATH = "/home/airflow/.local/bin:/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/home/airflow/.yarn/bin"

    def __init__(
        self,
        *,
        dag_id: str = "oa_web_workflow",
        start_date: Optional[pendulum.DateTime] = pendulum.DateTime(2021, 5, 2),
        schedule_interval: Optional[str] = "@weekly",
        catchup: Optional[bool] = False,
        ext_dag_id: str = "doi",
        table_ids: List[str] = None,
        airflow_vars: List[str] = None,
        retries: int = 3,
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
        self.add_operator(
            BashOperator(
                task_id="build_website",
                params={"website_folder": self.website_folder()},
                bash_command="cd {{ params.website_folder }} && ./build.sh ",
                retries=retries,
            )
        )
        self.add_operator(
            BashOperator(
                task_id="deploy_website",
                params={"website_folder": self.website_folder()},
                env={
                    "CF_API_TOKEN": get_airflow_connection_password(self.AIRFLOW_CONN_CLOUDFLARE_API_TOKEN),
                    "PATH": self.DEPLOY_WEBSITE_PATH,
                },
                bash_command="cd {{ params.website_folder }} && ./deploy.sh ",
                retries=retries,
            )
        )

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

        return OaWebRelease(dag_id=self.dag_id, project_id=project_id, release_date=release_date)

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
        base_path = os.path.join(release.transform_folder, "data")
        os.makedirs(base_path, exist_ok=True)
        auto_complete = []
        for category in self.table_ids:
            csv_path = os.path.join(release.download_folder, f"{category}.csv")
            df = pd.read_csv(csv_path)
            df.fillna("", inplace=True)
            ts_data = release.make_timeseries_data(df, category)
            df_index_table = release.make_index_table_data(df, ts_data, category)
            auto_complete += release.make_auto_complete_data(df_index_table, category)
            release.save_entity_details_data(df_index_table, category)

        # Save auto complete data as json
        output_path = os.path.join(base_path, "autocomplete.json")
        df_ac = pd.DataFrame(auto_complete)
        df_ac.to_json(output_path, orient="records")
