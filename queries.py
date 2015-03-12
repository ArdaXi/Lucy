import math

def usersearch(message, ignored):
  return {"query": {"filtered": {
            "query": {"multi_match": {"query": message,
                                      "fields": ["body",
                                                 "nick"]}},
            "filter": {"not": {"terms": {"nick": ignored}}}}}}

def when(nick, message):
  return {"query": {"filtered": {"query": {"match": {"body": message}},
                                 "filter": {"term": {"nick": nick}}}}}

def context(numid):
  return {"filter": {"range": {"numid": {"gte": numid-3,
                                         "lte": numid+3}}}}

def search(message, decay, numid, ignored):
  return { "_source": ["body"],
           "query": {
             "filtered": {
               "query": {
                 "function_score": {
                   "query": {
                     "match": {
                       "body": message
                     }
                   },
                   "functions": [{
                     "gauss": {
                       "numid": {
                         "decay": decay,
                         "origin": 1,
                         "offset": 1,
                         "scale": math.floor(numid / 2)
                       }
                     }
                   }, {
                     "linear": {
                       "mentions": {
                         "origin": 0,
                         "scale": 1
                       }
                     }
                   }]
                 }
               },
               "filter": {
                 "bool": {
                   "must_not": [{
                     "terms": {
                       "nick": ignored
                     }
                   }, {
                     "range": {
                       "numid": {
                         "gte": numid - 1000
                       }
                     }
                   }, {
                     "range": {
                       "date": {
                         "gt": "now-1d"
                       }
                     }
                   }]
                 }
               }
             }
           }
         }

def who(message, ignored):
  return { "query": {"filtered": { "query": {"match": {"body": message}},
                                   "filter": {
                                     "not": {"terms": {"nick": ignored}}}}},
           "aggs": { "nicks": {"terms": {"field": "nick"}}}}

# vim: tabstop=2:softtabstop=2:shiftwidth=2:expandtab
