import json


class MarketMap:
    def __init__(self):
        self.cbsacode = ""
        self.urlslug = ""
        self.zipprofiles = []

    def convert_to_dict(self):
        self.demographics.demographicquickfacts = json.loads(json.dumps(self.demographics.demographicquickfacts, default=lambda o: o.__dict__))

class MarketMapData:
    def __init__(self):
        self.geoid = ""
        self.medianhouseholdincome = 0
        self.demographics = {}
        self.hasData = True

