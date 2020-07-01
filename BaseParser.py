import json

from JoinCensusWithTweets import GeoShape, CensusLoader, Integrator
# Parse json file
from utils import MongoDbUtils, GraphDbUtils


class BaseParser:

    def __init__(self, json_filename, verbose=0):
        self.filename = json_filename
        self.verbose = verbose
        self.all_gdf, self.main_crs = GeoShape.load_shape_files()

    def extract_tweets(self):
        tweets = []
        data_package = []

        # integrate self.main_crs in tweets
        with open(self.filename, 'rb') as input_file:
            count = 0
            for line in input_file:
                obj = json.loads(line)
                tweet = GraphDbUtils.make_data(obj)
                tweet_package = MongoDbUtils.make_data(obj)

                if len(tweet) != 0:
                    tweets.append(tweet)
                if len(tweet_package) != 0:
                    data_package.append(tweet_package)
                if self.verbose == 1:
                    # print(count, '. \t ', tweet)
                    print(count, '. \t ', tweet_package)
                    count = count + 1

        return tweets, data_package

    def extract_census_data(self):
        all_census_df = CensusLoader.load_census_concat()
        pop_census_gdf = Integrator.join_census_shpfile(self.all_gdf, all_census_df)
        return pop_census_gdf
