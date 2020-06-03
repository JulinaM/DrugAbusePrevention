from dbModels.MongoDbService import MongoDbService


class MongoDbModel:

    def __init__(self, mongoClient, database, verbose=0):
        try:
            self.db = mongoClient[database]
            self.verbose = verbose
        except Exception as e:
            print(str(e))

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

    def update(self, id, data, tableName):
        try:
            collection = self.db[tableName]
            collection.update_one({'id': id}, data, upsert=True)
        except Exception as e:
            print(str(e))

    def select(self, tableName):
        try:
            col = self.db[tableName].find()
            print('\n All data from EmployeeData Database \n')
            for emp in col:
                print(emp)
        except Exception as e:
            print(str(e))

    def find_from_list(self, tableName, fieldName, valueList):
        try:
            results = self.db[tableName].find({fieldName: {"$in": valueList}})
            if self.verbose == 1:
                for record in results:
                    print(record)
            return results
        except Exception as e:
            print(str(e))


if __name__ == "__main__":
    mongoDbService = MongoDbService('mongodb://localhost:27017/')
    userIds = ["488427368", "26131727", "25642054", "25642054", "25642054", "25642054", "25642054"]
    mongoDbModel = MongoDbModel(mongoDbService.client, 'Twitter', 1)
    mongoDbModel.find_from_list("user", "id_str", userIds)
    mongoDbService.close()
