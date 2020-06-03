from dbModels.GraphDbModel import GraphDbModel
from dbModels.GraphDbService import GraphDbService
from dbModels.MongoDbModel import MongoDbModel
from dbModels.MongoDbService import MongoDbService

graphDbService = GraphDbService("bolt://localhost:7687", "neo4j", "test", 1)
graphModel = GraphDbModel(graphDbService.driver, 1)

mongoDbService = MongoDbService('mongodb://localhost:27017/')
mongoDbModel = MongoDbModel(mongoDbService.client, 'Twitter', 0)


def closeAll():
    graphDbService.close()
    mongoDbService.close()
