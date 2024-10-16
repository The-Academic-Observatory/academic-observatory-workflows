import os

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer


FIXTURES_DIR = "/app/fixtures"


def start_ftp_server():
    """Creates an FTP server on port 5021.

    Allows both authorised and anonymous logins.
    You can add a user and give them access to a specific directory namespace
    """

    authorizer = DummyAuthorizer()
    authorizer.add_anonymous(FIXTURES_DIR, perm="elrdfmwMT")
    authorizer.add_user("user", "password", FIXTURES_DIR, perm="elr")  # Basic user with access to home dir
    authorizer.add_user("pubmed", "password", os.path.join(FIXTURES_DIR, "pubmed"), perm="elrT")  # Pubmed

    handler = FTPHandler
    handler.permit_foreign_addresses = True
    handler.authorizer = authorizer
    handler.passive_ports = range(30000, 30009)

    server = FTPServer(("0.0.0.0", 5021), handler)
    server.serve_forever()


if __name__ == "__main__":
    start_ftp_server()
