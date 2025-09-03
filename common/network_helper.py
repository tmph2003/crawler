import requests
import time
import re

from logging import Logger
from config.config import config

log = Logger(name="network_helper")

class NetworkHelper:
    def __init__(self):
        """Initialize with a requests session."""
        self.session = requests.Session()

    def get_proxy(self):
        """
        Fetch proxy from API and return it as string.
        Assumes API returns JSON like {"proxyhttp": "http://ip:port"}.
        """
        url = f"https://proxyxoay.org/api/get.php?key={config.KEY_PROXY_ROATE}&nhamang=random&&tinhthanh=0"
        response = self.session.get(url, timeout=120)

        # Nếu API trả về JSON
        try:
            data = response.json()
            if data.get("status") == 100:
                print(f"Proxy: {data.get("proxyhttp")}")
                return data.get("proxyhttp").split(":")
            elif data.get("status") == 101:
                match = re.search(r"(\d+)s", data.get("message"))
                if match:
                    seconds = int(match.group(1))
                    time.sleep(seconds)
            else:
                log.error("Chưa đổi được proxy")
            
        except ValueError:
            # Nếu API trả về plain text (VD: "http://ip:port")
            raise ValueError
        finally:
            self.session.close()
