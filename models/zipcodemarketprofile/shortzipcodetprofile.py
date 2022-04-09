from models.shortneighborhoorprofile import shortdemographics
from models.shortneighborhoorprofile import shorteconomy
from models.shortneighborhoorprofile import shorthousing
import json

class ShortNeighborhoodProfile:
    def __init__(self):
        self.geoid = ""
        self.stateid = ""
        self.countyfullcode = ""
        self.cbsacode = ""
        self.geoshapecoordinates = []
        self.demographics = shortdemographics.Demographics()
        self.economy = shorteconomy.Economy()
        self.housing = shorthousing.Housing()

    def convert_to_dict(self):
        self.demographics.oneyeargrowth = json.loads(json.dumps(self.demographics.oneyeargrowth, default=lambda o: o.__dict__))

        self.economy.medianhouseholdincome = json.loads(json.dumps(self.economy.medianhouseholdincome, default=lambda o: o.__dict__))
        self.economy.unemploymentrate = json.loads(json.dumps(self.economy.unemploymentrate, default=lambda o: o.__dict__))
        self.housing.occupancyrate = json.loads(json.dumps(self.housing.occupancyrate, default=lambda o: o.__dict__))

        self.demographics = json.loads(json.dumps(self.demographics, default=lambda o: o.__dict__))
        self.economy = json.loads(json.dumps(self.economy, default=lambda o: o.__dict__))
        self.housing = json.loads(json.dumps(self.housing, default=lambda o: o.__dict__))
        # self.marketprofile = json.loads(json.dumps(self.marketprofile, default=lambda o: o.__dict__))

        return self

