from datetime import datetime
from threading import Thread
from collections import deque
import irc.bot
import json
import pyelasticsearch
import random
import re
import logging
import time
import math

strip_pattern = re.compile("[^\w ']+", re.UNICODE)
logging.basicConfig(level=logging.INFO)

class IgnoreErrorsBuffer(irc.buffer.DecodingLineBuffer):

  def __init__(self, *args, **kwargs):
    super(IgnoreErrorsBuffer, self).__init__(*args, **kwargs)
    self.logger = logging.getLogger("IgnoreErrorsBuffer")

  def handle_exception(self):
    self.logger.exception("Unicodey.")

class Lucy(irc.bot.SingleServerIRCBot):

  def __init__(self, config):
    with open(config) as f:
      config = json.load(f)
    server, port, nick = config['server'], config['port'], config['nick']
    irc.bot.SingleServerIRCBot.__init__(self, [(server, port)], nick, nick)
    self.connection.buffer_class = IgnoreErrorsBuffer
    self.channel, self.index = config['channel'], config['index']
    self.chance, self.ignored = config['chance'], config['ignored']
    self.queuelen, self.queueminlen = config['queuelen'], config['queueminlen']
    self.es = pyelasticsearch.ElasticSearch(config['elasticsearch'])
    self.numid = self.es.count("*", index=self.index)['count']
    self.logger = logging.getLogger("Lucy")
    self.queue = deque(maxlen=self.queuelen)
    self.counter, self.lastmsg = 0, 0
    
  def on_nicknameinuse(self, c, e):
    c.nick(c.get_nickname() + "_")
  
  def on_welcome(self, c, e):
    c.join(self.channel)

  def on_pubmsg(self, c, e):
    message = " ".join(e.arguments)
    args = message.split(" ")
    self.log(e.source.nick, message)
    if e.source.nick.lower() not in self.ignored:
      self.queue.append(strip_pattern.sub(' ', message))
      self.counter += 1
    if args[0].strip(",:") == c.get_nickname():
      if len(args) > 1:
        if args[1] == "search":
          Thread(target=self.usersearch,
                 args=(c, " ".join(args[2:]))).start()
          return
        if args[1] == "lastmsg":
          Thread(target=self.getlastmsg, args=(c,)).start()
          return
        if args[1] == "when":
          if len(args) > 3:
            Thread(target=self.when, args=(c, args[2], " ".join(args[3:]))).start()
            return
        if args[1] == "context":
          Thread(target=self.context, args=(c,)).start()
          return
    if c.get_nickname() in message or (self.counter >= self.queueminlen and
                                       len(self.queue) >= self.queueminlen and
                                       random.random() < self.chance):
      Thread(target=self.search, args=(c, list(self.queue))).start()
      self.counter = 0

  def on_join(self, c, e):
    pass

  def on_nick(self, c, e):
    pass

  def chan_msg(self, c, message):
    c.privmsg(self.channel, message)
    self.log(c.get_nickname(), message)

  def search(self, c, messages):
    message = " ".join(messages).replace(c.get_nickname(), '')
    try:
      query = {"_source": ["body"],
               "query":
               {"filtered": {"query": {"function_score": {"query": {"match": {"body": message}},
                                                          "functions": [{"gauss": {"numid": {"origin": 1,
                                                                                             "offset": 1,
                                                                                             "scale": math.floor(self.numid/
                                                                                                                2)}}},
                                                                        {"linear": {"mentions": {"origin": 0,
                                                                                                 "scale": 1}}}]}},
                             "filter": {"bool": {
                                         "must_not": [
                                          {"terms": {"nick": self.ignored}},
                                          {"range": {"numid":
                                                      {"gte": self.numid-1000}}},
                                          {"range": {"date":
                                                      {"gt": "now-1d"}}}]}}}}}
      result = self.es.search(query, index=self.index)
      for hit in result["hits"]["hits"]:
        score, source, id = hit["_score"], hit["_source"], hit["_id"]
        body = source["body"]
        self.logger.info("'{}' has score {}".format(body, score))
        self.lastmsg = id
        time.sleep(body.count(" ") * 0.2 + 0.5)
        self.chan_msg(c, body)
        self.incrementmsg(id)
        return
    except:
      self.logger.exception("Failed ES")
    self.queue.extendleft(reversed(messages[len(self.queue):]))

  def incrementmsg(self, id):
    script = "(ctx._source.mentions==null)?(ctx._source.mentions=1):(ctx._source.mentions+=1)"
    self.es.update(self.index, "message", id, script)

  def usersearch(self, c, message):
    try:
      query = {"_source": ["body", "date", "nick"],
               "query": {"multi_match": {"query": message,
                                         "fields": ["body",
                                                    "nick"]}}}
      result = self.es.search(query, index=self.index, size=5)
      hits = result["hits"]["hits"]
      total = result["hits"]["total"]
      self.sayhits(c, hits, total)
    except:
      self.logger.exception("Failed ES")

  def when(self, c, nick, message):
    try:
      nick = nick.lower()
      query = {"_source": ["body", "date", "nick"],
               "query": {"filtered": {"query": {"match": {"body": message}},
                                      "filter": {"term": {"nick": nick}}}}}
      result = self.es.search(query, index=self.index, size=5)
      hits = result["hits"]["hits"]
      total = result["hits"]["total"]
      self.sayhits(c, hits, total)
    except:
      self.logger.exception("Failed ES")

  def sayhits(self, c, hits, total):
    if total > len(hits):
      self.chan_msg(c, "{} results, showing {}.".format(total, len(hits)))
    else:
      self.chan_msg(c, "{} results.".format(total))
    error = False
    for hit in hits:
      score, source = hit["_score"], hit["_source"]
      body, date, nick = source["body"], source["date"], source["nick"]
      timestamp = datetime.strptime(date.split(".")[0], "%Y-%m-%dT%H:%M:%S")
      if score == 1.0:
        msg = "{:%Y-%m-%d %H:%M} <{}> {}".format(timestamp, nick, body)
      else:
        msg = "{:.4} {:%Y-%m-%d %H:%M} <{}> {}".format(score, timestamp,
                                                       nick, body)
      self.chan_msg(c, msg)

  def getlastmsg(self, c):
    if self.lastmsg == 0:
      self.chan_msg(c, "I haven't even said anything!")
      return
    try:
      result = self.es.get(self.index, "message", self.lastmsg)
      source = result["_source"]
      body, date, nick = source["body"], source["date"], source["nick"]
      timestamp = datetime.strptime(date.split(".")[0], "%Y-%m-%dT%H:%M:%S")
      msg = "{:%Y-%m-%d %H:%M} <{}> {}".format(timestamp, nick,
                                               body)
      self.chan_msg(c, msg)
    except:
      self.logger.exception("Failed ES")

  def context(self, c):
    if self.lastmsg == 0:
      self.chan_msg(c, "I haven't even said anything!")
      return
    try:
      result = self.es.get(self.index, "message", self.lastmsg)
      source = result["_source"]
      numid = source["numid"]
      query = {"filter": {"range": {"numid": {"gte": numid-3,
                                              "lte": numid+3}}}}
      result = self.es.search(query, index=self.index)
      hits = sorted(result["hits"]["hits"],
                    key=self.numid_from_hit)
      total = result["hits"]["total"]
      self.sayhits(c, hits, total)
    except:
      self.logger.exception("Failed ES")

  def numid_from_hit(self, hit):
    return hit["_source"]["numid"]

  def log(self, nick, message):
    doc = {'numid': self.numid, 'date': datetime.now().isoformat(),
           'nick': nick, 'body': message}
    self.es.index(self.index, "message", doc)
    self.numid += 1

if __name__ == "__main__":
  bot = Lucy("config.json")
  try:
    bot.start()
  except KeyboardInterrupt:
    bot.die()
    raise

# vim: tabstop=2:softtabstop=2:shiftwidth=2:expandtab
