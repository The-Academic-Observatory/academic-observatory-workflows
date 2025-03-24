These instructions are for the 2024-08-23 release. Go to this Zenodo page to find the latest version: https://zenodo.org/records/13376773

Download the Make Data Count Data Citation Corpus:
```bash
wget https://zenodo.org/records/13376773/files/2024-08-23-data-citation-corpus-v2.0.zip
```

Extract archive:
```bash
unzip 2024-08-23-data-citation-corpus-v2.0.zip
```

Make folder to save transformed data:
```bash
mkdir transform
```

Transform from JSON to JSON Lines:
```bash
for file in ./json/*.json; do jq -c '.[]' "$file" > "./transform/$(basename "$file")l"; done
```

Upload to Google Cloud Storage bucket:
```bash
gsutil -m  cp -r ./transform/* gs://your-bucket-name/data_citation_corpus/2024-08-23/
```

Finally, load the BigQquery table using the schema/data_citation_corpus.json and the files on the Cloud Storage bucket,
e.g. using the BigQuery UI.