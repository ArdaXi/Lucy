import irc.bot
import json
import pyelasticsearch
from datetime import datetime
import random
import sys
import re
import logging
from threading import Thread
import time
from collections import deque
from datetime import datetime
import math

strip_pattern = re.compile("[^\w ']+", re.UNICODE)
logging.basicConfig(level=logging.INFO)

class Lucy(irc.bot.SingleServerIRCBot):
  def __init__(self, config):
    with open(config) as f:
      config = json.load(f)
    server, port, nick = config['server'], config['port'], config['nick']
    irc.bot.SingleServerIRCBot.__init__(self, [(server, port)], nick, nick)
    self.channel = config['channel']
    self.index = config['index']
    self.chance = config['chance']
    self.ignored = config['ignored']
    self.queuelen = config['queuelen']
    self.queueminlen = config['queueminlen']
    self.es = pyelasticsearch.ElasticSearch(config['elasticsearch'])
    self.numid = self.es.count("*", index=self.index)['count']
    self.logger = logging.getLogger(__name__)
    self.queue = deque(maxlen=self.queuelen)
    
  def on_nicknameinuse(self, c, e):
    c.nick(c.get_nickname() + "_")
  
  def on_welcome(self, c, e):
    c.join(self.channel)

  def on_pubmsg(self, c, e):
    message = " ".join(e.arguments)
    self.log(e.source.nick, message)
    if e.source.nick not in self.ignored:
      self.queue.append(strip_pattern.sub(' ', message))
    if c.get_nickname() in message or (len(self.queue) >= self.queueminlen and
                                       random.random() < self.chance):
      Thread(target=self.search, args=(c, list(self.queue))).start()
      self.queue.clear()

  def on_join(self, c, e):
    pass

  def on_nick(self, c, e):
    pass

  def chan_msg(self, c, message):
    c.privmsg(self.channel, message)
    self.log(c.get_nickname(), message)

  def search(self, c, messages):
    message = " ".join(messages).encode("utf-8")
    try:
      query = {"_source": ["body", "date"],
               "query":
               {"filtered": {"query": {"function_score": {"query": {"match": {"body": message}},
                                                          "gauss": {"numid": {"origin": 1,
                                                                              "offset": 1,
                                                                              "scale": math.floor(self.numid/2)}}}},
                             "filter": {"bool": {
                                         "must_not": [
                                          {"terms": {"nick": self.ignored}},
                                          {"range": {"numid":
                                                      {"gte": self.numid-1000}}
                                          }]}}}}}
      result = self.es.search(query, index=self.index)
      #threshold = message.count(" ") / len(messages) + 0.9
      threshold = 0.1
      for hit in result["hits"]["hits"]:
        score, source = hit["_score"], hit["_source"]
        body, date = source["body"], source["date"]
        if score < threshold:
          self.logger.info("'{}' has score {}, threshold: {}".format(body.encode("utf-8"),
                                                                     score,
                                                                     threshold))
          break
        timestamp = datetime.strptime(date.split(".")[0], "%Y-%m-%dT%H:%M:%S")
        delta = datetime.now() - timestamp
        if delta.total_seconds() < 10800:
          continue
        self.logger.info("'{}' has score {}".format(body, score))
        time.sleep(body.count(" ") * 0.2 + 0.5)
        self.chan_msg(c, body)
        return
    except:
      self.logger.exception("Failed ES")
    self.queue.extendleft(reversed(messages[len(self.queue):]))

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
