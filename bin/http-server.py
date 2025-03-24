import os

from flask import Flask, send_file, request, abort

app = Flask(__name__)

FIXTURES_DIR = "/app/fixtures"


@app.route("/crossref_metadata/snapshots/monthly/2022/12/all.json.tar.gz")
def crossref_metadata_download():

    filepath = os.path.join(FIXTURES_DIR, "crossref_metadata/crossref_metadata.json.tar.gz")
    return send_file(filepath, as_attachment=True)


@app.route("/unpaywall/1/feed/snapshot")
def unpaywall_snapshot():
    api_key = request.args.get("api_key")
    if api_key != "secret":
        abort(403)
    filepath = os.path.join(FIXTURES_DIR, "unpaywall/unpaywall_snapshot_2023-04-25T083002.jsonl.gz")
    return send_file(filepath, as_attachment=True)


@app.route("/unpaywall/<_>/daily-feed/changefile/<filename>")
def unpaywall_changefiles_get(_: str, filename: str):
    api_key = request.args.get("api_key")
    if api_key != "secret":
        abort(403)
    filepath = os.path.join(FIXTURES_DIR, f"unpaywall/{filename}")
    return send_file(filepath, as_attachment=True)


@app.route("/")
def site_map():
    ret = []
    ret.append("---- Testing HTTP Server ----")
    ret.append("Available endpoints:")
    for rule in app.url_map.iter_rules():
        ret.append(f"{rule}")
    return "\n".join(ret)


# @app.route("/")  # For debugging
# def hello_world():
#     return "hello world"


if __name__ == "__main__":
    """
    This is an application to serve test files to the e2e tests.

    Debug the available routes with `flask --app flask-app.py routes`
    """

    # Start the flask HTTP server
    app.run(host="0.0.0.0", port=5080, debug=True)
