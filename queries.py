import math

def usersearch(message, ignored):
  return { "query": {"filtered": {
             "query": {"multi_match": {"query": message,
                                       "fields": ["body",
                                                  "nick"]}},
             "filter": {"not": {"terms": {"nick": ignored}}}}},
           "highlight": {"pre_tags": ["_"], "post_tags": ["_"],
                         "fields": {"body": {}}}}

def when(nick, message):
  if message:
    query = when(nick, None)
    query["query"]["filtered"]["query"] = {"match": {"body": message}}
    return query
  else:
    return {"query": {"filtered": {"filter": {"term": {"nick": nick}}}}}

def context(numid):
  return {"filter": {"range": {"numid": {"gte": numid-3,
                                         "lte": numid+3}}}}

def who(message, ignored):
  if message:
    query = who(None, ignored)
    query["query"]["filtered"]["query"] = {"match": {"body": message}}
    query["aggs"]["nicks"]["terms"]["min_doc_count"] = 1
    return query
  else:
    return { "query": {"filtered": { "filter": {
                                       "not": {"terms": {"nick": ignored}}}}},
             "aggs": { "nicks": {"terms": {"field": "nick", "min_doc_count": 1000}}}}

def regex(expression):
  return {"query": {"regexp": {"body": expression}}}

def significant(nick):
  return {"query": {"filtered": {"filter": {"term": {"nick": nick}}}},
          "aggs": {"most_sig": {"significant_terms": {"field": "body"}}}}


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

# vim: tabstop=2:softtabstop=2:shiftwidth=2:expandtab
