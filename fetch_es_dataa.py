import sys
import os
import time
import configparser as cp
#from utils import bcolors
import ast
from pandas.io.json import json_normalize
global hello_df
global df

try:
    import pandas as pd
except:
    print("Please make sure you have pandas installed. pip -r requirements.txt or pip install pandas")
    sys.exit(0)

try:
    from elasticsearch import Elasticsearch, helpers
except:
    print("Please make sure you have elasticsearch module installed. pip -r requirements.txt or pip install elasticsearch")
    sys.exit(0)

period=24
#config_default = os.path.join(os.path.dirname(__file__), 'config/report.cfg')
print("Python system version:[" + str(sys.version_info) + "]")


class FinalReport:
    def __init__(self,
                 es_index='*',
                 es_host='localhost',
                 es_port=9200,
                 es_timeout=100,
                 period=24
                 ):

        self.es_host         = es_host
        self.es_index        = es_index
        self.es_port         = es_port
        self.es_timeout      = es_timeout
        self.period          = period
        
        try:
            print(' Attempting to connect to elasticsearch...')
            #self.es = Elasticsearch(self.es_host, port=self.es_port, timeout=self.es_timeout, http_auth=(self.auth_user, self.auth_password), verify_certs=False)
            self.es = Elasticsearch(self.es_host, port=self.es_port, timeout=self.es_timeout)
            print(' Connected to elasticsearch on ')
        except Exception as e:
            print(e)
            raise Exception("Could not connect to ElasticSearch -- Please verify your settings are correct and try again.")
        #self.hello_df=self.run_query()
        #print(type(self.hello_df))
        #print(str(self.hello_df.columns))
      
    def query(self,num_hours):
        # Timestamp in ES is in milliseconds
        NOW     = int(time.time() * 1000)
        SECONDS = 1000
        MINUTES = 60 * SECONDS
        HOURS   = 60 * MINUTES
        lte     = NOW
        gte     = int(NOW - num_hours * HOURS)
        
        query ={
                "query": {
                 "bool": {
                   "must": [
                     {
                       "match_all": {}
                          },
                           {
                            "exists": {
                              "field": "title.author.keyword"
                                }
                                 },
                                  {
                                   "range": {
                                    "@timestamp": {
                                     "gte": gte,
                                     "lte": lte,
                                     "format": "epoch_millis"
                                        }
                                       }
                                      }
                                     ]
                                    }
                                   } 
                                 }
        return query

    def run_query(self):
        global df
        period=24
        q = self.query(self.period)
        print("\n")
        print("\t" + str(q))
        print("\n")
        resp = helpers.scan(query=q, client=self.es, scroll="90m", index=self.es_index, timeout="10m",preserve_order=True)
        print(resp)
        df   = pd.DataFrame.from_dict([rec['_source'] for rec in resp])
        print(df.columns)
        if len(df) == 0:
            raise Exception("Elasticsearch did not retrieve any data. Please ensure your settings are correct inside the config file.")
 
        return df

    
def main():
    es_host='ip_address'
    es_index='index'
    es_port=9200
    es_timeout=480
    period=24
    #
   
    #

    eb = FinalReport(es_index,es_host,es_port, es_timeout,period=24)
    hello_df=eb.run_query()
    print(str(hello_df.columns))
    print()
    
if __name__ == '__main__':
    main()
