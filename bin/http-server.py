import os

from flask import Flask, Response, send_file, request, abort, url_for

app = Flask(__name__)

FIXTURES_DIR = "/app/fixtures"


@app.route("/crossref_metadata/snapshots/monthly/2022/12/all.json.tar.gz")
def crossref_metadata_download():

    filepath = os.path.join(FIXTURES_DIR, "crossref_metadata/crossref_metadata.json.tar.gz")
    return send_file(filepath, as_attachment=True)


@app.route("/unpaywall/1/feed/snapshot")
def unpaywall_snapshot_redirect():
    api_key = request.args.get("api_key")
    if api_key != "secret":
        print(f"Unauthorized access attempt with api_key: {api_key}")
        abort(403)

    filename = "unpaywall_snapshot_2023-04-25T083002.jsonl.gz"
    redirect_url = url_for("serve_unpaywall_file", filename=filename, _external=True)
    print(f"Redirecting to: {redirect_url} for filename: {filename}")

    # Create a Response object for the redirect. The status code is 302 (Found) for a temporary redirect.
    response = Response(status=302)
    response.headers["Location"] = redirect_url
    return response


@app.route("/unpaywall/download/<filename>")
def serve_unpaywall_file(filename):
    # You might add checks here for tokens, expiry, or specific referrer if needed
    # for security in a production environment.
    print(f"Attempting to serve file: {filename}")
    filepath = os.path.join(FIXTURES_DIR, "unpaywall", filename)

    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        abort(404)  # File not found

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
