import os

from flask import Flask, send_file

app = Flask(__name__)

FIXTURES_DIR = "/app/fixtures"


@app.route("/snapshots/monthly/2022/12/all.json.tar.gz")
def crossref_metadata_download():

    filepath = os.path.join(FIXTURES_DIR, "crossref_metadata.json.tar.gz")
    return send_file(filepath, as_attachment=True)


@app.route("/")  # For debugging
def hello_world():
    return "hello world"


if __name__ == "__main__":
    """
    This is an application to serve test files to the e2e tests.

    Debug the available routes with `flask --app flask-app.py routes`
    """
    app.run(host="0.0.0.0", port=5000, debug=True)
