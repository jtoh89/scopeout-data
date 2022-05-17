import json
from models.geojson import GeoJson
from globals import COLOR_LEVEL_NA

class ZipcodeGeoJsonProperties:
    def __init__(self):
        self.geoid = ""
        self.mediansaleprice = None
        self.mediansalepriceyoy = None
        self.mediansalepricemom = None
        self.dom = None

class ZipcodeMarketMap:
    def __init__(self):
        self.cbsacode = ""
        self.cbsaname = ""
        self.urlslug = ""
        self.coordinates = None
        self.geojson = None
        self.mediansalepricecolors = ["match", ["get", "geoid"]]
        self.mediansalepriceyoycolors = ["match", ["get", "geoid"]]
        self.mediansalepricemomcolors = ["match", ["get", "geoid"]]
        self.domcolors = ["match", ["get", "geoid"]]
        self.mediansalepricelegend = LegendDetails()
        self.mediansalepriceyoylegend = LegendDetails()
        self.mediansalepricemomlegend = LegendDetails()
        self.domlegend = LegendDetails()

    def convert_to_dict(self):
        self.mediansalepricelegend = json.loads(json.dumps(self.mediansalepricelegend, default=lambda o: o.__dict__))
        self.mediansalepriceyoylegend = json.loads(json.dumps(self.mediansalepricelegend, default=lambda o: o.__dict__))
        self.mediansalepricemomlegend = json.loads(json.dumps(self.mediansalepricemomlegend, default=lambda o: o.__dict__))
        self.geojson = json.loads(json.dumps(self.geojson, default=lambda o: o.__dict__))
        self.domlegend = json.loads(json.dumps(self.domlegend, default=lambda o: o.__dict__))


class LegendDetails:
    def __init__(self):
        self.level0color = COLOR_LEVEL_NA
        self.level0description = "Not Available"
        self.level1color = ""
        self.level1description = ""
        self.level2color = ""
        self.level2description = ""
        self.level3color = ""
        self.level3description = ""
        self.level4color = ""
        self.level4description = ""
        self.level5color = ""
        self.level5description = ""

