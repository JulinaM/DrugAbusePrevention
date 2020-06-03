from DEMO import graphModel, mongoDbModel, closeAll


# DEMO 1 : Query all the users that were mentioned in tweets with hashtag #goodfellow
def demo1():
    result = graphModel.getResultsWithHashtag("goodfellow")
    print(result)
    users = mongoDbModel.find_from_list("user", "id", result['users'])
    for user in users:
        print({user["id"], user["name"]})


# DEMO 1 : Query all the tweets that has hashtag #goodfellow
def demo2():
    result = graphModel.getResultsWithHashtag("goodfellow")
    tweets = mongoDbModel.find_from_list("tweet", "id", result['tweets'])
    for tweet in tweets:
        print({tweet["id"], tweet["text"]})


# DEMO 3 : Count nodes in Graph DB
def demo3():
    graphModel.getCount('tweet')
    graphModel.getCount('user')
    graphModel.getCount('hashtag')


if __name__ == "__main__":
    demo1()
    print('\n')
    demo2()
    print('\n')
    demo3()
    closeAll()
