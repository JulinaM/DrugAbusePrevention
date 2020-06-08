import copy
import json

import geojson


def make_data(json_obj):
    tweet_map = {'tweet': _process_tweet(json_obj), 'user': _process_user(json_obj), 'place': _process_place(json_obj)}
    entities = _process_entity(json_obj)
    tweet_map['urls'] = entities['urls']
    tweet_map['media'] = entities['media']
    tweet_map['hashtags'] = entities['hashtags']
    tweet_map['symbols'] = entities['symbols']

    json_data = json.dumps(tweet_map)
    return json_data


def _process_user(json_obj):
    data = json_obj['user']
    userId = data['id']
    user = {'id': userId, 'data': data}
    return user


def _process_tweet(json_obj):
    json_copy = copy.deepcopy(json_obj)
    del json_copy['user']
    del json_copy['entities']
    del json_copy['place']
    tweetId = json_copy['id']
    tweet = {'id': tweetId, 'data': json_copy}
    return tweet


def _process_place(json_obj):
    data = json_obj['place']
    try:
        if data['bounding_box']['type'] == 'Polygon':
            polygon = geojson.Polygon(data['bounding_box']['coordinates'])
        if not polygon.is_valid and polygon.errors() == 'Each linear ring must end where it started':
            for index in range(len(polygon['coordinates'])):
                polygon['coordinates'][index].append(polygon['coordinates'][index][0])
            if polygon.is_valid:
                data['bounding_box'] = polygon
            else:
                print('Invalid Polygon Error', polygon.errors())

    except Exception as e:
        print(str(e))

    placeId = data['id']
    place = {'id': placeId, 'data': data}
    return place


def _process_entity(json_obj):
    entities = json_obj['entities']
    hashtags_data = entities['hashtags']
    # media_data = entities['media']
    urls_data = entities['urls']
    symbols_data = entities['symbols']
    urls = {'id': '', 'data': urls_data}
    # medias = {'id': '', 'data': media_data}
    hashtags = {'id': '', 'data': hashtags_data}
    symbols = {'id': '', 'data': symbols_data}
    # usermentions = []
    return {'urls': urls, 'media': '', 'hashtags': hashtags, 'symbols': symbols}
