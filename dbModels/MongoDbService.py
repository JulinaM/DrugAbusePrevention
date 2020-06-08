from pymongo import MongoClient

COLLECTIONS = ['tweet', 'user', 'place', 'hashtags', 'symbol', 'url']
INDEX_MAP = {'tweet': ['id'],
             'user': ['id', 'screen_name'],
             'place': ['id', 'name'],
             # 'hashtags': []
             }


class MongoDbService:

    def __init__(self, uri):
        try:
            self.client = MongoClient('localhost', 27017)
        except Exception as e:
            print(str(e))

    def close(self):
        self.client.close()
