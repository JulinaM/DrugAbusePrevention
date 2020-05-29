import json


def make_data(json_obj: json) -> json:
    userId = json_obj['user']['id']
    tweetId = json_obj['id']

    if tweetId and userId:
        tweet = {'id': tweetId, 'userId': userId}
        entities = json_obj['entities']

        if json_obj['user']['location']:
            tweet['locationId'] = json_obj['user']['location']
        else:
            tweet['locationId'] = 'l'

        if json_obj['place']['id']:
            tweet['placeId'] = json_obj['place']['id']

        if entities['hashtags']:
            hashtags = []
            for each_tag in entities['hashtags']:
                hashtags.append(each_tag['text'])
            tweet['hashtags'] = hashtags

        if entities['symbols']:
            symbols = []
            for each_symbol in entities['symbols']:
                symbols.append(each_symbol['text'])
            tweet['symbols'] = symbols

        if entities['urls']:
            urls = []
            for each_url in entities['urls']:
                urls.append(each_url['url'])
            tweet['urls'] = urls

        if entities['user_mentions']:
            user_mentions = []
            for each_usr in entities['user_mentions']:
                user_mentions.append(each_usr['id'])
            tweet['user_mentions'] = user_mentions

        # if entities['media']:
        #     media = []
        #     for each_media in entities['media']:
        #         media.append(each_media['id'])
        #     tweet['media'] = media

        return tweet
    else:
        print('--Empty-- TweetId: {} and UserId: {} \t'.format(tweetId, userId))
        return {}
