import json
from models.geojson import GeoJson


class TractMarketGeoJsonProperties:
    def __init__(self):
        self.geoid = ""
        self.medianhouseholdincome = None
        # self.medianhouseholdincomecolor = "#999999"
        self.unemploymentrate = None
        # self.unemploymentratecolor = "#999999"
        self.owneroccupancyrate = None
        # self.owneroccupancyratecolor = "#999999"

class TractMarketMap:
    def __init__(self):
        self.cbsacode = ""
        self.cbsaname = ""
        self.urlslug = ""
        self.coordinates = None
        self.geojson = GeoJson()
        self.medianhouseholdincomecolors = ["match", ["get", "geoid"]]
        self.unemploymentratecolors = ["match", ["get", "geoid"]]
        self.owneroccupancyratecolors = ["match", ["get", "geoid"]]
        self.medianhouseholdincomelegend = LegendDetails()
        self.unemploymentratelegend = LegendDetails()
        self.owneroccupancyratelegend = LegendDetails()

    def convert_to_dict(self):
        self.medianhouseholdincomelegend = json.loads(json.dumps(self.medianhouseholdincomelegend, default=lambda o: o.__dict__))
        self.geojson = json.loads(json.dumps(self.geojson, default=lambda o: o.__dict__))
        self.unemploymentratelegend = json.loads(json.dumps(self.unemploymentratelegend, default=lambda o: o.__dict__))
        self.owneroccupancyratelegend = json.loads(json.dumps(self.owneroccupancyratelegend, default=lambda o: o.__dict__))


class LegendDetails:
    def __init__(self):
        self.level0color = "#999999"
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

