class GeoSpatialQueryBuilder:

    @staticmethod
    def build_circleQuery(lat, lng, radi):
        query = {
            "bounding_box": {
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
            "bounding_box": {
                "$geoWithin": {
                    "$polygon": coordinates
                }
            }
        }
        return query

    @staticmethod
    def build_intersectQuery(geoType, coordinates):
        query = {
            "bounding_box": {
                "$geoIntersects": {
                    "$geometry": {
                        "type": geoType,
                        "coordinates": coordinates
                    }
                }
            }
        }
        return query
