# Open Access Website Workflow
A workflow that generates the data for the COKI Open Access Website and uploads the data to a Google Cloud Storage
bucket, which is used to build the Next.js web application. Each time the workflow runs successfully a repository 
dispatch event is created, this event triggers the Github actions responsible for building the web application.

+------------------------------+--------------------------------------+
| Summary                      |                                      |
+==============================+======================================+
| Frequency                    | Default: @weekly                     |
+------------------------------+--------------------------------------+
| Runs on remote worker        | Default: False                       |
+------------------------------+--------------------------------------+
| Catchup missed runs          | Default: False                       |
+------------------------------+--------------------------------------+
| Credentials Required         | Yes                                  |
+------------------------------+--------------------------------------+
| Uses Workflow Template       | Workflow                             |
+------------------------------+--------------------------------------+

The figure below illustrates the generated data and notes about what each file is used for.
```
    .
    ├── data: data
    │   ├── autocomplete.json: used for the website search functionality. Copied into public/data folder.
    │   ├── autocomplete.parquet: used for filtering in Cloudflare Worker.
    │   ├── country: individual entity statistics files for countries. Used to build each country page.
    │   │   ├── ALB.json
    │   │   ├── ARE.json
    │   │   └── ARG.json
    │   ├── country.json: used to create the country table. First 18 countries used to build first page of country table
    │   │                 and then this file is included in the public folder and downloaded by the client to enable the
    │   │                 other pages of the table to be displayed. Copied into public/data folder.
    │   ├── country.jsonl: used to generate the parquet file.
    │   ├── country.parquet: to be used along with apache-arrow to enable filtering from a Cloudflare Worker.
    │   ├── institution: individual entity statistics files for institutions. Used to build each institution page.
    │   │   ├── 05ykr0121.json
    │   │   ├── 05ym42410.json
    │   │   └── 05ynxx418.json
    │   ├── institution.json: used to create the institution table. First 18 institutions used to build first page of institution table
    │   │                     and then this file is included in the public folder and downloaded by the client to enable the
    │   │                     other pages of the table to be displayed. Copied into public/data folder.
    │   ├── institution.jsonl: used to generate the parquet file.
    │   ├── institution.parquet: to be used along with apache-arrow to enable filtering from a Cloudflare Worker.
    │   └── stats.json: global statistics, e.g. the minimum and maximum date for the dataset, when it was last updated etc.
    └── logos: country and institution logos. Copied into public/logos folder.
        ├── country
        │   ├── l: large logos displayed on country pages.
        │   │   ├── ALB.svg
        │   │   ├── ARE.svg
        │   │   └── ARG.svg
        │   └── s: small logos displayed in country table.
        │       ├── ALB.svg
        │       ├── ARE.svg
        │       └── ARG.svg
        └── institution
            ├── l: large logos displayed on institution pages.
            │   ├── 05ykr0121.jpg
            │   ├── 05ym42410.jpg
            │   └── 05ynxx418.jpg
            └── s: small logos displayed in institution table.
                ├── 05ykr0121.jpg
                ├── 05ym42410.jpg
                └── 05ynxx418.jpg
```

## Airflow Variables
In the config.yaml file, the following Airflow variables are required.

### oa_web_data_bucket
The name of the Google Cloud Storage bucket where the data will be uploaded.

When you create the bucket, enable object versioning to keep previous releases.

## Airflow Connections
In the config.yaml file, the following Airflow connections are required.

### oa_web_github_token
The Github PAT token that is used to send repository dispatch events.

Create a new Github user account and give it write permissions to the [coki-oa-web](https://github.com/The-Academic-Observatory/coki-oa-web)
repository.

To create the Github PAT, in the new user account, go to: Settings > Developer Settings > Person access tokens and Generate new token.

Give the token the `public_repo` permission.