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
    self.es = pyelasticsearch.ElasticSearch(config['elasticsearch'])
    self.numid = self.es.count("*", index=self.index)['count']
    self.logger = logging.getLogger(__name__)
    self.queue = deque(maxlen=10) #TODO: Make configurable
    
  def on_nicknameinuse(self, c, e):
    c.nick(c.get_nickname() + "_")
  
  def on_welcome(self, c, e):
    c.join(self.channel)

  def on_pubmsg(self, c, e):
    message = " ".join(e.arguments)
    self.log(e.source.nick, message)
    self.queue.append(strip_pattern.sub(' ', message))
    if len(self.queue) >= 5 and random.random() < self.chance:
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
      result = self.es.search("body:({})".format(message), index=self.index)
      #threshold = message.count(" ") / len(messages) + 0.9
      threshold = 0.3
      for hit in result["hits"]["hits"]:
        score, body = hit["_score"], hit["_source"]["body"]
        if score < threshold:
          self.logger.info("'{}' has score {}, threshold: {}".format(body, score, threshold))
          self.queue.extendleft(reversed(messages[len(self.queue):]))
          return
    #    if score > threshold:
    #      self.logger.info("'{}' has score {}, threshold: {}".format(body, score, threshold))
    #      continue
        if strip_pattern.sub(' ', body) in messages:
          continue
        self.logger.info("'{}' has score {}".format(body, score))
        time.sleep(body.count(" ") * 0.5 + 0.5)
        self.chan_msg(c, body)
        return
    except:
      e, msg = sys.exc_info()[:2]
      if e == IndexError:
        return
      self.logger.exception("Failed ES")

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
