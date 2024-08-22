Make folders:
```bash
mkdir datacite transform
```

Download DataCite Public Data File:
```bash
cd datacite && wget https://datafiles.datacite.org/datafiles/public-2023/download?token=YOUR_TOKEN
```

Extract DataCite Public Data File:
```bash
cd datacite && tar -xzf DataCite_Public_Data_File_2023.tar.gz
```

Install dependencies and 
```bash
pip3 install json_lines bigquery_schema_generator jsonlines
nohup python3 datacite_transform.py /path/to/datacite /path/to/transform > output.log 2>&1 &
```

Upload transformed files to Google Cloud Storage bucket:
```bash
gsutil -m  cp -r /path/to/transform/* gs://your-bucket-name/datacite/2023/
```

Finally, load the DataCite BigQquery table using the schema/datacite.json and the files on the Cloud Storage bucket,
e.g. using the BigQuery UI.