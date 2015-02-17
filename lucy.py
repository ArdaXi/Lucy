import irc.bot
import json
import pyelasticsearch
from datetime import datetime
import random
import sys
import re
import logging

strip_pattern = re.compile('[^\w ]+', re.UNICODE)
logging.basicConfig(level=logging.INFO)

class Lucy(irc.bot.SingleServerIRCBot):
  def __init__(self, config):
    with open(config) as f:
      config = json.load(f)
    server, port, nick = config['server'], config['port'], config['nick']
    irc.bot.SingleServerIRCBot.__init__(self, [(server, port)], nick, nick)
    self.channel = config['channel']
    self.index = config['index']
    self.es = pyelasticsearch.ElasticSearch(config['elasticsearch'])
    self.numid = self.es.count("*", index=self.index)['count']
    self.logger = logging.getLogger(__name__)
    
  def on_nicknameinuse(self, c, e):
    c.nick(c.get_nickname() + "_")
  
  def on_welcome(self, c, e):
    c.join(self.channel)

  def on_pubmsg(self, c, e):
    self.log(e.source.nick, " ".join(e.arguments))
    if(random.random() > 0.5):
      self.search(c, e)

  def on_join(self, c, e):
    pass

  def on_nick(self, c, e):
    pass

  def chan_msg(self, c, message):
    c.privmsg(self.channel, message)
    self.log(c.get_nickname(), message)

  def search(self, c, e):
    message = strip_pattern.sub('', " ".join(e.arguments))
    try:
      result = self.es.search("body:({})".format(message), index=self.index)
      for hit in result["hits"]["hits"]:
        score, body = hit["_score"], hit["_source"]["body"]
        if score < 1.0:
          return
        if score > len(e.arguments) - 0.1:
          continue
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
