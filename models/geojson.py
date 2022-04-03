import json

class GeoJson:
    def __init__(self):
        self.type = "FeatureCollection"
        self.name = "marketgeojson"
        self.features = []

class GeoJsonFeature:
    def __init__(self):
        self.id = ""
        self.type = "Feature"
        self.properties = None
        self.geometry = GeoJsonGeometry()

    #
    # def convert_to_dict(self):
    #     self.properties = json.loads(json.dumps(self.properties, default=lambda o: o.__dict__))
    #     self.geometry = json.loads(json.dumps(self.geometry, default=lambda o: o.__dict__))


class GeoJsonGeometry:
    def __init__(self):
        self.type = "MultiPolygon"
        self.coordinates = []