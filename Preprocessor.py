import json

from BaseParser import BaseParser
from dbModels.GraphDbModel import GraphDbModel
from dbModels.GraphDbService import GraphDbService
from dbModels.MongoDbModel import MongoDbModel
from dbModels.MongoDbService import MongoDbService
from dbModels.MySQLModel import MySQLModel
from dbModels.MySQLService import MySQLService


def main():
    filename = "nys_tweets_filtered_2017_0.json"
    # filename = "test"
    filepath = "./data/tweets/"
    tweet_filename = f"{filepath}{filename}"

    parser = BaseParser(tweet_filename)
    tweets, data_package = parser.extract_tweets()

    graphDbService = GraphDbService("bolt://localhost:7687", "neo4j", "test", 1)
    graphModel = GraphDbModel(graphDbService.driver, 1)
    graphModel.insert(tweets)
    graphDbService.close()

    mongoDbService = MongoDbService('mongodb://localhost:27017/')
    mongoDbModel = MongoDbModel(mongoDbService.client, 'Twitter', 1)
    for each_data in data_package:
        json_data = json.loads(each_data)
        for tableName in json_data.keys():
            value = json_data[tableName]
            if len(value) != 0:
                mongoDbModel.insert(value['id'], value['data'], tableName)
    mongoDbService.close()

    pop_census_gdf = parser.extract_census_data()
    mySQLService = MySQLService('localhost', 'root', 'password')
    mySQLModel = MySQLModel(mySQLService.connection)
    mySQLModel.insert(pop_census_gdf, 'census2010')
    mySQLService.close()


if __name__ == '__main__':
    main()
