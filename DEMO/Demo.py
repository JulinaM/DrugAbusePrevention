from DEMO import graphModel, mongoDbModel, closeAll


# DEMO 1 : Query all the users that were mentioned in tweets with hashtag #goodfellow
def demo1():
    userIds = graphModel.getListOfUsers("goodfellow")
    mongoDbModel.find_from_list("user", "id", userIds)


if __name__ == "__main__":
    demo1()
    closeAll()
