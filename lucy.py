from datetime import datetime
from threading import Thread, Lock
from collections import deque
from inspect import getmembers, isfunction
from importlib import reload
import irc.bot
import json
import pyelasticsearch
import random
import re
import logging
import time
import math

import commands

strip_pattern = re.compile("[^\w ']+", re.UNICODE)
logging.basicConfig(level=logging.INFO)

class IgnoreErrorsBuffer(irc.buffer.DecodingLineBuffer):

  def __init__(self, *args, **kwargs):
    super(IgnoreErrorsBuffer, self).__init__(*args, **kwargs)
    self.logger = logging.getLogger("IgnoreErrorsBuffer")

  def handle_exception(self):
    self.logger.exception("Unicodey.")

class Lucy(irc.bot.SingleServerIRCBot):

  def __init__(self, configfile):
    self.configfile = configfile
    with open(configfile) as f:
      config = json.load(f)
    server, port, nick = config['server'], config['port'], config['nick']
    self.reload(config)
    irc.bot.SingleServerIRCBot.__init__(self, [(server, port)], nick, nick)
    self.connection.buffer_class = IgnoreErrorsBuffer
    self.channel, self.index = config["channel"], config['index']
    self.es = pyelasticsearch.ElasticSearch(config['elasticsearch'])
    self.numid = self.es.count("*", index=self.index)['count']
    self.logger = logging.getLogger("Lucy")
    self.queue = deque(maxlen=self.queuelen)
    self.counter, self.lastmsg = 0, 0
    self.mention_lock = Lock()

  def is_public_function(self, o):
    return isfunction(o) and not o.__name__.startswith('_')

  def reload(self, config=None):
    self.commands = {}
    reload(commands)
    self.commands = dict(getmembers(commands, self.is_public_function))
    if not config:
      with open(self.configfile) as f:
        config = json.load(f)
    self.admins, self.decay = config["admins"], config["decay"]
    self.chance, self.ignored = config['chance'], config['ignored']
    self.queuelen, self.queueminlen = config['queuelen'], config['queueminlen']
    
  def on_nicknameinuse(self, c, e):
    c.nick(c.get_nickname() + "_")
  
  def on_welcome(self, c, e):
    c.join(self.channel)

  def on_pubmsg(self, c, e):
    nick = e.source.nick
    message = " ".join(e.arguments)
    args = message.split(" ")
    self.log(nick, message)
    if nick.lower() not in self.ignored:
      self.queue.append(strip_pattern.sub(' ', message))
      self.counter += 1
    if args[0].strip(",:") == c.get_nickname():
      if len(args) > 1:
        if nick in self.admins and args[1] == "reload":
          self.reload()
          self.chan_msg(c, "Reloaded.")
          return
        if args[1] in self.commands:
          target = self.commands[args[1]]
          Thread(target=target, args=(self, c, args[2:])).start()
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

  def searchquery(self, message):
    return {"_source": ["body"],
            "query":
            {"filtered": {"query": {"function_score": {"query": {"match": {"body": message}},
                                                       "functions": [{"gauss": {"numid": {"decay": self.decay,
                                                                                          "origin": 1,
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

  def say_searchquery(self, c, messages):
    message = " ".join(messages).replace(c.get_nickname(), '')
    query = self.searchquery(message)
    subquery = query["query"]["filtered"]["query"]
    filter = query["query"]["filtered"]["filter"]
    self.chan_msg(c, '{"query": {"filtered": {')
    self.chan_msg(c, '"query": {},'.format(json.dumps(subquery)))
    self.chan_msg(c, '"filter": {}'.format(json.dumps(filter)))
    self.chan_msg(c, '}}}')

  def search(self, c, messages):
    message = " ".join(messages).replace(c.get_nickname(), '')
    try:
      query = self.searchquery(message)
      with self.mention_lock:
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
    with self.mention_lock:
      script = "ctx._source.mentions+=1"
      self.es.update(self.index, "message", id, script)
      time.sleep(1)

  def sayhits(self, c, hits, total):
    if total > len(hits):
      self.chan_msg(c, "{} results, showing {}.".format(total, len(hits)))
    else:
      self.chan_msg(c, "{} results.".format(total))
    error = False
    for hit in hits:
      score, source, id = hit["_score"], hit["_source"], hit["_id"]
      body, date, nick = source["body"], source["date"], source["nick"]
      timestamp = datetime.strptime(date.split(".")[0], "%Y-%m-%dT%H:%M:%S")
      if score == 1.0:
        msg = "{} {:%Y-%m-%d %H:%M} <{}> {}".format(id, timestamp,
                                                    nick, body)
      else:
        msg = "{} {:.4} {:%Y-%m-%d %H:%M} <{}> {}".format(id, score,
                                                          timestamp, nick,
                                                          body)
      self.chan_msg(c, msg)

  def log(self, nick, message):
    doc = {'numid': self.numid, 'date': datetime.now().isoformat(),
           'nick': nick, 'body': message, 'mentions': 0}
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
