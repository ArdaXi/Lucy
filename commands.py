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
    took = result["took"]
    hits = result["hits"]["hits"]
    total = result["hits"]["total"]
    _sayhits(parent, c, hits, total, took)
  except:
    logger.exception("Failed ES")

def explain(parent, c, args):
  try:
    result = parent.es.send_request("GET", [parent.index, "message",
                                            parent.lastmsg, "_explain"],
                                    parent.lastquery, {})
    if not result["matches"]:
      parent.chan_msg(c, "Universe broken, please destroy and try again.")
    expl = result["explanation"]
    msg = "{:.2} {}".format(expl["value"], expl["description"])
    parent.chan_msg(c, msg)
    for expl in expl["details"]:
      msg = "{:.2} {}".format(expl["value"], expl["description"])
      parent.chan_msg(c, msg)
  except:
    logger.exception("Failed ES")

def when(parent, c, args):
  if not args:
    return
  nick = args[0]
  message = " ".join(args[1:]) if len(args) > 1 else None
  try:
    query = queries.when(nick, message)
    result = parent.es.search(query, index=parent.index, size=5)
    took = result["took"]
    hits = result["hits"]["hits"]
    total = result["hits"]["total"]
    _sayhits(parent, c, hits, total, took)
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
    took = result["took"]
    hits = sorted(result["hits"]["hits"],
                  key=_numid_from_hit)
    total = result["hits"]["total"]
    _sayhits(parent, c, hits, total, took)
  except:
    logger.exception("Failed ES")

def who(parent, c, args):
  try:
    message = " ".join(args) if args else None
    query = queries.who(message, parent.ignored)
    result = parent.es.search(query, index=parent.index, es_search_type="count", size=20)
    total = result["hits"]["total"]
    buckets = result["aggregations"]["nicks"]["buckets"]
    values = ["{}: {:.0%}".format(x["key"], x["doc_count"] / total)
               for x in buckets if x["doc_count"] / total > 0.01]
    msg = ", ".join(values)
    parent.chan_msg(c, msg)
    parent.chan_msg(c, "Total: {}".format(total))
  except:
    logger.exception("Failed ES")

def regex(parent, c, args):
  try:
    query = queries.regex(args[0])
    result = parent.es.search(query, index=parent.index, size=5)
    took = result["took"]
    hits = result["hits"]["hits"]
    total = result["hits"]["total"]
    _sayhits(parent, c, hits, total, took)
  except:
    logger.exception("Failed ES")

def significant(parent, c, args):
  if not args:
    return
  try:
    nick = args[0]
    query = queries.significant(nick)
    result = parent.es.search(query, index=parent.index, es_search_type="count")
    took = result["took"]
    buckets = result["aggregations"]["most_sig"]["buckets"]
    words = [x["key"] for x in buckets]
    msg = ", ".join(words)
    parent.chan_msg(c, msg)
  except:
    logger.exception("Failed ES")

def _numid_from_hit(hit):
  return hit["_source"]["numid"]


def _sayhits(parent, c, hits, total, took):
  if total > len(hits):
    parent.chan_msg(c, "{} results, showing {}. ({} ms)".format(total,
                                                                len(hits),
                                                                took))
  else:
    parent.chan_msg(c, "{} results. ({} ms)".format(total, took))
  for hit in hits:
    score, source, id = hit["_score"], hit["_source"], hit["_id"]
    body, date, nick = source["body"], source["date"], source["nick"]
    if "highlight" in hit:
      body = hit["highlight"]["body"][0]
    timestamp = datetime.strptime(date.split(".")[0], "%Y-%m-%dT%H:%M:%S")
    msg = "{} {:%Y-%m-%d %H:%M} <{}> {}".format(id, timestamp,
                                                nick, body)
    if score != 1.0:
      msg = "{:.4} {}".format(score, msg)
    parent.chan_msg(c, msg)

# vim: tabstop=2:softtabstop=2:shiftwidth=2:expandtab
