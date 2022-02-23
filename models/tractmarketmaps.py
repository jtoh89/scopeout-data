# from models.demographics import



class TractMarketMap:
    def __init__(self):
        self.cbsacode = ""
        self.urlslug = ""
        self.tractprofiles = []

    # def convert_to_dict(self):
    #     self.demographics.demographicquickfacts = json.loads(json.dumps(self.demographics.demographicquickfacts, default=lambda o: o.__dict__))

class TractMarketMapData:
    def __init__(self):
        self.geoid = ""
        self.medianhouseholdincome = None
        self.medianhouseholdincomecolor = "#999999"
        self.unemploymentrate = None
        self.unemploymentratecolor = "#999999"
        self.geometry = []