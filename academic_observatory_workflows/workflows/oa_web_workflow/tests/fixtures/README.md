# Creating updated test files

## Schemas
Export country schema:
```bash
bq show --schema --format=prettyjson  --project_id=academic-observatory observatory.country20220108 > ./academic_observatory_workflows/fixtures/oa_web_workflow/schema/country.json
```

Export institution schema:
```bash
bq show --schema --format=prettyjson --project_id=academic-observatory observatory.institution20220108 > ./academic_observatory_workflows/fixtures/oa_web_workflow/schema/institution.json
```

## country.jsonl
1) Run the following query in BigQuery, saving the result into a table. It is best to edit Query Settings to directly
save the table to a destination table, because the BigQuery UI hangs after displaying the query results.
```sql
SELECT * FROM `academic-observatory.observatory.country20220108`
WHERE id in ("AUS", "NZL") AND time_period >= 2020 AND time_period <= 2021
```

2) In BigQuery, navigate to the table, click export and save to Google Cloud Storage in JSON (Newline delimited) format
   with compression. The filename should be country.jsonl.gz.
   
3) Download country.jsonl into ./academic-observatory-workflows/academic_observatory_workflows/fixtures/oa_web_workflow

## institution.jsonl
1) Run the following query in BigQuery, saving the result into a table. It is best to edit Query Settings to directly
save the table to a destination table, because the BigQuery UI hangs after displaying the query results.
```sql
SELECT * FROM `academic-observatory.observatory.institution20220108`
WHERE id in ("https://ror.org/02n415q13", "https://ror.org/03b94tp07") AND time_period >= 2020 AND time_period <= 2021
```

2) In BigQuery, navigate to the table, click export and save to Google Cloud Storage in JSON (Newline delimited) format
   with compression. The filename should be institution.jsonl.gz.
   
3) Download institution.jsonl into ./academic-observatory-workflows/academic_observatory_workflows/fixtures/oa_web_workflow