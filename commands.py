import logging
from datetime import datetime
from irc.client import MessageTooLong

import queries

logger = logging.getLogger(__name__)

def search(parent, c, args):
  message = " ".join(args)
  try:
    query = queries.usersearch(message, parent.ignored)
    result = parent.es.search(query, index=parent.index, size=5)
    hits = result["hits"]["hits"]
    total = result["hits"]["total"]
    parent.sayhits(c, hits, total) 
  except:
    logger.exception("Failed ES")

def when(parent, c, args):
  if len(args) < 2:
    return
  try:
    nick = args[0].lower()
    message = " ".join(args[1:])
    query = queries.when(nick, message)
    result = parent.es.search(query, index=parent.index, size=5)
    hits = result["hits"]["hits"]
    total = result["hits"]["total"]
    parent.sayhits(c, hits, total)
  except:
    logger.exception("Failed ES")

def lastmsg(parent, c, args):
  if parent.lastmsg == 0:
    parent.chan_msg(c, "I haven't even said anything!")
    return
  try:
    result = parent.es.get(parent.index, "message", parent.lastmsg)
    source = result["_source"]
    body, date, nick = source["body"], source["date"], source["nick"]
    timestamp = datetime.strptime(date.split(".")[0], "%Y-%m-%dT%H:%M:%S")
    msg = "{} {:%Y-%m-%d %H:%M} <{}> {}".format(parent.lastmsg, timestamp,
                                                nick, body)
    try:
      parent.chan_msg(c, msg)
    except MessageTooLong:
      msg = "{} {:%Y-%m-%d %H:%M} <{}>".format(parent.lastmsg, timestamp,
                                               nick)
      parent.chan_msg(c, msg)
  except:
    logger.exception("Failed ES")

def context(parent, c, args):
  id = args[0] if args else parent.lastmsg
  if id == 0:
    return
  try:
    result = parent.es.get(parent.index, "message", id,
                           fields="numid")
    numid = result["fields"]["numid"][0]
    query = queries.context(numid) 
    result = parent.es.search(query, index=parent.index)
    hits = sorted(result["hits"]["hits"],
                  key=_numid_from_hit)
    total = result["hits"]["total"]
    parent.sayhits(c, hits, total)
  except:
    logger.exception("Failed ES")

def who(parent, c, args):
  query = queries.who(" ".join(args))
  result = parent.es.search(query, index=parent.index, es_search_type="count")
  total = result["hits"]["total"]
  buckets = result["aggregations"]["nicks"]["buckets"]
  values = ["{}: {:.0%}".format(x["key"], x["doc_count"] / total)
             for x in buckets]
  msg = ", ".join(values)
  parent.chan_msg(c, msg)

def _numid_from_hit(hit):
  return hit["_source"]["numid"]

# vim: tabstop=2:softtabstop=2:shiftwidth=2:expandtab
