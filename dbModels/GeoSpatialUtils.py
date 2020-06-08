class MongoDbGeoSpatialModel:

    @staticmethod
    def build_circleQuery(lat, lng, radi):
        query = {
            "loc": {
                "$near": {
                    "$geometry": {
                        "type": "point",
                        "coordinates": [lat, lng]
                    },
                    "$minDistance": 0,
                    "$maxDistance": radi
                }
            }
        }
        return query

    @staticmethod
    def build_polygonQuery(coordinates):
        # coordinates = [ [ <x1> , <y1> ], [ <x2> , <y2> ], [ <x3> , <y3> ], ... ]
        query = {
            "loc": {
                "$geoWithin": {
                    "$polygon": coordinates
                }
            }
        }
        return query
