import irc.bot
import json
import pyelasticsearch
from datetime import datetime
import random
import sys

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
    self.log(self.get_nickname(), message)

  def search(self, c, e):
    message = " ".join(e.arguments)
    try:
      result = self.es.search("body:({})".format(message), index=self.index)
      hit = result["hits"]["hits"][0]
      if hit["_score"] < 1.0 or hit["_score"] > len(message) - 0.1:
        return
      body = hit["_source"]["body"]
      if body == message:
          return
      self.chan_msg(c, body)
    except:
      e = sys.exc_info()[0]
      if e == IndexError:
        return
      self.chan_msg(c, e.__name__)

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
