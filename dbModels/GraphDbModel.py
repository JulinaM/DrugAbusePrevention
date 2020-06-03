from dbModels.GraphDbService import GraphDbService


class GraphDbModel:

    def __init__(self, driver, verbose=0):
        self.driver = driver
        self.verbose = verbose

    def insert(self, tweets):
        with self.driver.session() as session:
            for tweet in tweets:
                message = session.write_transaction(_create_and_return_tweet, tweet)
                if self.verbose == 1:
                    print(message)
                    # print(tweets)

    def getResultsWithHashtag(self, hashtag=''):
        with self.driver.session() as session:
            result = session.read_transaction(_fetchResult, hashtag)
            return {
                'users': [record['user'] for record in result],
                'tweets': [record['tweet'] for record in result]
            }

    def getCount(self, labelName):
        with self.driver.session() as session:
            result = session.read_transaction(_count, labelName)
            if self.verbose == 1:
                print(labelName, " count:", result)
            return result


def _create_and_return_tweet(tx, tweet):
    print(tweet)
    result = tx.run("WITH $tw as tweet "
                    "MERGE (t:tweet {id:tweet.id}) "
                    "MERGE (u:user {id:tweet.userId}) "
                    "MERGE (l:location {id:tweet.locationId}) "
                    "MERGE (p:place {id:tweet.placeId}) "
                    "MERGE (u)-[:at]->(l) "
                    "MERGE (u)-[:send]->(t) "
                    "MERGE (t)-[:tag]->(p) "
                    # "WITH tweet.hashtags AS nested,t,u,tweet unwind nested as val merge (t)-[:tag]->(h:hashtag{text:val}) "
                    # "WITH tweet.urls AS nested,t,u,tweet unwind nested as val merge (t)-[:include]->(url:url{url:val}) "
                    # "WITH tweet.symbols AS nested,t,u,tweet unwind nested as val merge (t)-[:has]->(s:symbol{text:val})"
                    # "WITH tweet.user_mentions AS nested,t,u,tweet unwind nested as val merge (t)-[:mention]->(u1:user{id:val}) "
                    , tw=tweet)

    # TODO : Try https://neo4j.com/docs/cypher-manual/current/clauses/call-subquery/
    tx.run("WITH $tw  as tweet "
           "MERGE (t:tweet {id:tweet.id}) "
           "WITH t, tweet unwind tweet.hashtags as hash MERGE (h:hashtag {text:hash}) "
           "MERGE (t) -[:tag]->(h)", tw=tweet)

    tx.run("WITH $tw  as tweet "
           "MERGE (t:tweet {id:tweet.id}) "
           "WITH t, tweet unwind tweet.user_mentions as val MERGE (u:user {id:val}) "
           "MERGE (t) -[:mention]->(u)", tw=tweet)

    tx.run("WITH $tw  as tweet "
           "MERGE (t:tweet {id:tweet.id}) "
           "WITH t, tweet unwind tweet.urls as val MERGE (url:url {url:val}) "
           "MERGE (t) -[:include]->(url)", tw=tweet)

    tx.run("WITH $tw  as tweet "
           "MERGE (t:tweet {id:tweet.id}) "
           "WITH t, tweet unwind tweet.symbols as val MERGE (s:symbol {text:val}) "
           "MERGE (t) -[:has]->(s)", tw=tweet)

    return result


def _fetchResult(tx, keyword):
    return list(
        tx.run(
            "MATCH (u:user)-[:send]-(t:tweet)-[:tag]-(h:hashtag) where h.text=~'(?i)goodfel.*' return u.id as user, t.id as tweet",
            keyword=keyword))


def _count(tx, labelName):
    CQL = 'MATCH (t:user) RETURN count(t)'
    if labelName == 'tweet':
        CQL = 'MATCH (t:tweet) RETURN count(t)'
    if labelName == 'hashtag':
        CQL = 'MATCH (t:hashtag) RETURN count(t)'

    return tx.run(CQL).single().value()


if __name__ == "__main__":
    graphDbService = GraphDbService("bolt://localhost:7687", "neo4j", "test", 1)
    graphModel = GraphDbModel(graphDbService.driver, 1)
    results = graphModel.getResultsWithHashtag('')
    print(results)
    graphModel.getCount('tweet')
    graphDbService.close()
