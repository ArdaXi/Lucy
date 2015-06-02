import requests
from httpcache import CachingHTTPAdapter
from datetime import datetime

URL = "https://openexchangerates.org/api/latest.json?app_id="
session = requests.Session()
session.mount('http://', CachingHTTPAdapter())
latest = (datetime.min, 0.0)

def dollar(key):
  global latest
  time, euro = latest
  delta = datetime.utcnow() - time
  if delta.total_seconds() < 3600:
    return latest
  response = session.get(URL, params={"app_id": key}).json()
  euro = response["rates"]["EUR"]
  time = datetime.utcfromtimestamp(response["timestamp"])
  latest = time, euro
  return latest
