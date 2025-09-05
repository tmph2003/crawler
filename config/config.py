import os


class AppConfig(object):
    """
    Access environment variables here.
    """

    def __init__(self):
        pass

    ENV = os.environ.get("ENV", "dev")
    MARIADB_HOST = os.environ.get("MARIADB_HOST", "localhost")
    MARIADB_PORT = os.environ.get("MARIADB_PORT", "3306")
    MARIADB_USER = os.environ.get("MARIADB_USER", "root")
    MARIADB_PASS = os.environ.get("MARIADB_PASS", "admin")

    AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minio")
    AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minio123")
    AWS_REGION = os.environ.get("AWS_REGION", "ap-southeast-1")
    BUCKET_NAME = os.environ.get("BUCKET_NAME", "crawl-data")

    KEY_PROXY_ROATE = os.environ.get("BUCKET_NAME", "abc")
    PROXY_URLS = os.environ.get("PROXY_URLS", [
        f"http://user18634:1756989016@103.179.188.215:18634/",
        f"http://user18733:1756989016@103.179.188.215:18733/"
    ])

config = AppConfig()