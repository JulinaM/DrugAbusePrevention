from dbModels.MongoDbService import MongoDbService


class MongoDbModel:

    def __init__(self, mongoClient, database, verbose=0):
        try:
            self.db = mongoClient[database]
            self.verbose = verbose
        except Exception as e:
            print(str(e))

    def create_geoIndex(self, tableName, locationFiled):
        self.db[tableName].createIndex({locationFiled: "2dsphere"})

    # Insert json array [{data1}, {data2}]
    def insert(self, id, data, tableName):
        try:
            collection = self.db[tableName]
            # if len(data) != 0:
            ids = collection.insert(data).inserted_ids
            if self.verbose == 1:
                print('Inserted {} in table {} \t res: {}'.format(data, tableName, ids))
            return ids

        except Exception as e:
            print(str(e))

    def find(self, tableName, query=None):
        if query is None:
            query = {}
        try:
            col = self.db[tableName].find(query)
            print('\n All data from EmployeeData Database \n')
            for emp in col:
                print(emp)
        except Exception as e:
            print(str(e))

    def find_from_list(self, tableName, fieldName, valueList):
        query = {fieldName: {"$in": valueList}}
        return self.find(tableName, query)

    def update(self, tableName, fieldName, fieldValue, data):
        filter_query = {fieldName: fieldValue}
        replaceBy = {"$set": data}
        try:
            collection = self.db[tableName]
            collection.update_one(filter_query, replaceBy)
        except Exception as e:
            print(str(e))


if __name__ == "__main__":
    mongoDbService = MongoDbService('mongodb://localhost:27017/')
    userIds = ["488427368", "26131727", "25642054", "25642054", "25642054", "25642054", "25642054"]
    mongoDbModel = MongoDbModel(mongoDbService.client, 'Twitter', 1)
    mongoDbModel.find_from_list("user", "id_str", userIds)
    mongoDbService.close()
