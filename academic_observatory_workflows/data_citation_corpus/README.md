Download the Make Data Count Data Citation Corpus:
```bash
wget https://zenodo.org/records/11216814/files/2024-05-10-data-citation-corpus-v1.1.zip?download=1
```

Extract archive:
```bash
unzip 2024-05-10-data-citation-corpus-v1.1.zip
```

Make folder to save transformed data:
```bash
mkdir transform
```

Transform from JSON to JSON Lines:
```bash
for file in ./2024-05-10-data-citation-corpus-v1.1/json/*.json; do jq -c '.[]' "$file" > "./transform/$(basename "$file")l"; done
```

Upload to Google Cloud Storage bucket:
```bash
gsutil -m  cp -r ./transform/* gs://your-bucket-name/data_citation_corpus/2024-05-10/
```

Finally, load the BigQquery table using the schema/data_citation_corpus.json and the files on the Cloud Storage bucket,
e.g. using the BigQuery UI.