from collections import Counter

import fiona
import geojson
import geopandas as gpd
import itertools
import json
import os
import pandas as pd
import pytz
# from zipfile import ZipFile
# from io import BytesIO, TextIOWrapper
import time
from datetime import datetime
from pytz import timezone
from shapely.geometry import Point
# from timezonefinderL import TimezoneFinder
# from pattern.en import lemma
from timezonefinderL import TimezoneFinder

from geo_loc_utils import get_state_fips_list, replace_sp_tokens, get_tokens
from twokenize import normalizeTextForTagger
from utils import GraphDbUtils, MongoDbUtils

time_format = "%a %b %d %H:%M:%S %z %Y"
time_format_db = "%Y-%m-%d %H:%M:%S"


# drug_category = read_drug_category()
# stopwords = set(read_stopwords())
# stopwords.add('%URL%')
# stopwords.add('%USER_MENTION%')
# stopwords.add('%QUOTES%')


def join_census_shpfile(all_gdf, census_df):
    pop_gdf = all_gdf.merge(census_df, on='GEOID')
    return pop_gdf


class GeoShape:

    @staticmethod
    def list_shape_files():
        state_fips_list = get_state_fips_list()
        home_path = './data/shpfiles/census_tracts_simple_2010/'
        filename_temp = 'gz_2010_{}_140_00_500k.zip'
        path_list = []
        for f in state_fips_list:
            path = home_path + filename_temp.format(f)
            if not os.path.isfile(path):
                raise FileNotFoundError('{} not found'.format(path))
                return None
            else:
                path_list.append(path)
        print('Scan complete, found census tract shapefiles of {} states'.format(len(state_fips_list)))
        return path_list

    @staticmethod
    def load_shape_files():
        def clip_geoid(x):
            return x[9:]

        path_list = GeoShape.list_shape_files()
        main_crs = None
        gdf_list = []
        s_time = time.time()
        total_cts = 0
        print('loading shape files', path_list)
        for path in path_list:
            inf = open(path, 'rb')
            direct = inf.read()
            with fiona.BytesCollection(direct) as f:
                crs = f.crs
                ct_gdf = gpd.GeoDataFrame.from_features([feature for feature in f], crs=crs)
                print("Shape of the ct_gdf: {}".format(ct_gdf.shape))
                print("Projection of ct_gdf: {}".format(ct_gdf.crs))
                ct_gdf = ct_gdf.to_crs({'init': 'epsg:4326'})
                # print("Projection of ct_gdf: {}".format(ct_gdf.crs))
                if not main_crs:
                    main_crs = ct_gdf.crs
                gdf_list.append(ct_gdf)
                total_cts += ct_gdf.shape[0]
            inf.close()
        print('loading done, takes {} seconds'.format(time.time() - s_time))
        print('{} states shpfiles are loaded, {} census tracts'.format(len(gdf_list), total_cts))
        # concat all geo dataframes
        all_gdf = gpd.GeoDataFrame(pd.concat(gdf_list, ignore_index=True), crs=main_crs)
        all_gdf['GEOID'] = all_gdf['GEO_ID'].apply(clip_geoid)
        print("Shape of the all_gdf: {}".format(all_gdf.shape))
        print("Projection of all_gdf: {}".format(all_gdf.crs))
        # print(all_gdf.loc[all_gdf['GEOID'] == '47053966500'])
        return all_gdf, main_crs


class CensusLoader:
    @staticmethod
    def load_census_concat():
        census_home_path = './data/census_2010/'
        census_filename = 'all_140_in_{}.P1.csv'
        state_fips_list = get_state_fips_list()
        census_df_list = []
        total_cts = 0
        for s in ['01']:
            print('----', census_home_path + census_filename.format(s))
            census_data = pd.read_csv(census_home_path + census_filename.format(s), sep=',')
            # usecols=['GEOID', 'POP100'], dtype={'GEOID': str, 'POP100': float})
            census_df_list.append(census_data)
            total_cts += census_data.shape[0]
        print('{} states census data are loaded, {} census tracts'.format(len(census_df_list), total_cts))
        all_census_df = pd.concat(census_df_list)
        print("shape of concat census df: {}".format(all_census_df.shape))
        return all_census_df


class TweetLoader:

    @staticmethod
    def parse_fields_2017_coord(tweet_obj):
        # parse files of 2017 data, where coord exists
        tzfinder = TimezoneFinder()
        utc = pytz.utc

        def parse_text(tw_obj):
            # remove use mentions, urls from the text
            # use extended tweet if presents
            if 'extended_tweet' in tw_obj:
                text = tw_obj['extended_tweet']['full_text']
            # or use normal text
            else:
                text = tw_obj['text']

            # process quoted tweet and append to text
            if tw_obj['is_quote_status'] and 'quoted_status' in tw_obj:
                # process quoted tweet
                qt_obj = tw_obj['quoted_status']
                if 'extended_tweet' in qt_obj:
                    qt_text = qt_obj['extended_tweet']['full_text']
                # or use normal text
                else:
                    qt_text = qt_obj['text']
                text = ''.join([text, ' %QUOTES% ', qt_text])

            text_norm = normalizeTextForTagger(replace_sp_tokens(text))
            # process text into list of keywords
            text_tokens = get_tokens(text)
            # text_tokens = [t for t in text_tokens if t not in stopwords]
            token_counts = dict(Counter(itertools.chain(*[text_tokens])))
            # text_tokens = [lemma(t) for t in text_tokens]

            return text, text_norm, text_tokens, token_counts

        def parse_time(tw_obj):
            # parse timestamp to needed format
            # we need an actual timestamp in utc
            # and a fake local timestamp in utc
            # get timezone info
            point = tw_obj['coordinates']['coordinates']
            try:
                tz_name = tzfinder.timezone_at(lng=point[0], lat=point[1])
                tz_info = timezone(tz_name)
            except Exception as e:
                # if there is error when converting timezone
                # give default timezone as: US/Central
                tz_name = 'US/Central'
                tz_info = timezone(tz_name)
            # parse the utc timestamp
            time_obj = datetime.strptime(tw_obj['created_at'], time_format)
            # convert to local timestamp
            local_time_obj = time_obj.astimezone(tz_info)
            # get local hour mark for "time-of-day" query
            hour_mark = local_time_obj.time().hour
            # make a fake local timestamp with UTC timezone
            fake_time_obj = utc.localize(local_time_obj.replace(tzinfo=None))
            return time_obj, local_time_obj, fake_time_obj, hour_mark, tz_name

        def parse_category(tw_obj):
            # parse category into str
            return 'Positive'
            # label = int(row['label'])
            # if label == 1:
            #   return 'Positive'
            # else:
            #   return 'Negative'

        #
        # def parse_drug_category(text_norm):
        #     cats = {}
        #     for cat in drug_category:
        #         for t in drug_category[cat]:
        #             if t in text_norm:
        #                 if cat in cats:
        #                     cats[cat].append(t)
        #                 else:
        #                     cats[cat] = [t]
        #     return cats

        result_dict = {}
        time_obj, local_time_obj, fake_time_obj, hour_mark, tz_name = parse_time(tweet_obj)
        try:
            text, text_norm, text_tokens, token_counts = parse_text(tweet_obj)
        except:
            print(tweet_obj)
            return None
        # put up dict
        result_dict['tweetID'] = tweet_obj['id_str']
        result_dict['utcTime'] = time_obj
        # actual local time is not neede since db always saves utc
        # result_dict['localTime'] = local_time_obj
        result_dict['fakeLocalTime'] = fake_time_obj
        # result_dict['hourMark'] = hour_mark
        result_dict['timezone'] = tz_name
        result_dict['Followers'] = tweet_obj['user']['followers_count']
        result_dict['Friends'] = tweet_obj['user']['friends_count']
        result_dict['Statuses'] = tweet_obj['user']['statuses_count']
        result_dict['textRaw'] = text
        result_dict['textNorm'] = text_norm
        result_dict['textTokens'] = text_tokens
        result_dict['textTokenCounts'] = token_counts
        # result_dict['drugCategory'] = parse_drug_category(text_norm)
        result_dict['geo'] = tweet_obj['coordinates']
        result_dict['geometry'] = Point(tweet_obj['coordinates']['coordinates'])
        result_dict['category'] = parse_category(tweet_obj)
        return result_dict

    def to_Point(self, x):
        return Point(geojson.loads(x)['coordinates'])

    @staticmethod
    def load_tweets_from_json(p):
        with open(p, 'r', encoding='utf-8') as inf:
            j_str = inf.readline()
        return json.loads(j_str)

    @staticmethod
    def parseGeo(tweet_obj):
        tweet_obj['geometry'] = Point(tweet_obj['coordinates']['coordinates'])
        return tweet_obj


class Integrator:

    # join census data with shpfile, get a geodataframe with population and geometry
    @staticmethod
    def join_census_shpfile(all_gdf, census_df):
        pop_gdf = all_gdf.merge(census_df, on='GEOID')
        return pop_gdf

    @staticmethod
    def build_gdf_from_processed_tweets(tweet_json, main_crs):
        result_dict_list = []
        for tw_obj in tweet_json:
            # td = parse_fields_2017_coord(tw_obj)
            td = TweetLoader.parseGeo(tw_obj)
            if td is not None:
                result_dict_list.append(td)

        df = pd.DataFrame.from_records(result_dict_list)
        tweet_gdf = gpd.GeoDataFrame(df, crs=main_crs)
        return tweet_gdf

    # join tweets with already joined census data and shpfiles
    @staticmethod
    def join_tweets_pop_gdf(pop_gdf, tweet_gdf):
        join_gdf = gpd.sjoin(tweet_gdf, pop_gdf, how='left', op='within', lsuffix='left', rsuffix='right')
        return join_gdf

    @staticmethod
    def process(tweetFile):
        all_gdf, main_crs = GeoShape.load_shape_files()

        all_census_df = CensusLoader.load_census_concat()

        pop_gdf = Integrator.join_census_shpfile(all_gdf, all_census_df)

        # file = './data/tweets/2017_positive_tweets_24.json'
        # tweet_json = TweetLoader.load_tweets_from_json(file)

        # tweet_gdf = Integrator.build_gdf_from_processed_tweets(tweet_json, main_crs)

        # joined_gdf = Integrator.join_tweets_pop_gdf(pop_gdf, tweet_gdf)

        # joined_gdf = joined_gdf.rename({'POP100': 'Pop_2010',
        #                                 'GEOID': 'GeoID_2010',
        #                                 'TRACT': 'TractID_2010',
        #                                 'STATE': 'StateID',
        #                                 'COUNTY': 'CountyID_2010'}, axis='columns')
        # joined_gdf = joined_gdf[['Followers', 'Friends', 'Statuses',
        #                          'category', 'drugCategory', 'fakeLocalTime',
        #                          'geo', 'textNorm', 'textRaw', 'textTokenCounts',
        #                          'textTokens', 'timezone', 'tweetID', 'utcTime',
        #                          'Pop_2010', 'GeoID_2010', 'TractID_2010',
        #                          'StateID', 'CountyID_2010']]

        return joined_gdf


if __name__ == "__main__":
    all_gdf, main_crs = GeoShape.load_shape_files()
    all_census_df = CensusLoader.load_census_concat()
    pop_gdf = Integrator.join_census_shpfile(all_gdf, all_census_df)

    file = "data/tweets/2017_positive_tweets_24.json"
    tweet_json = TweetLoader.load_tweets_from_json(file)
    tweet_gdf = Integrator.build_gdf_from_processed_tweets(tweet_json, main_crs)
    joined_gdf = Integrator.join_tweets_pop_gdf(pop_gdf, tweet_gdf)
    #
    # print(joined_gdf)
    # df = joined_gdf.head()
    count = 0
    for jdict in joined_gdf.to_dict(orient='records'):
        print(jdict)
        tweet = GraphDbUtils.make_data(jdict)
        print("---", tweet)
        tweet_package = MongoDbUtils.make_data(jdict)
        count = count + 1
        if count == 5:
            break
