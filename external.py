import requests
from httpcache import CachingHTTPAdapter
from datetime import datetime

URL = "https://openexchangerates.org/api/latest.json?app_id="
session = requests.Session()
session.mount('http://', CachingHTTPAdapter())
latest = (datetime.min, 0.0)

def dollar(key):
  global latest
  old_time, old_euro = latest
  delta = datetime.utcnow() - old_time
  if delta.total_seconds() < 3600:
    return "USD is still worth {} EUR, ask me again later.".format(old_euro)
  response = session.get(URL, params={"app_id": key}).json()
  time = datetime.utcfromtimestamp(response["timestamp"])
  euro = response["rates"]["EUR"]
  latest = time, euro
  diff = euro - old_euro
  if diff == 0:
    return "USD is still worth {} EUR, ask me again later.".format(old_euro)
  verb = "gained" if diff > 0 else "lost"
  perc_diff = abs(diff) / old_euro
  return "USD has {} {:.2%} value since {} and is currently worth {} EUR".format(
           verb, perc_diff, "some point in time", euro)
