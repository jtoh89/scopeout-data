from models import demographics
from models import economy
from models import housing
import json

class NeighborhoodProfile:
    def __init__(self):
        self.geoid = ""
        self.stateid = ""
        self.countyfullcode = ""
        self.cbsacode = ""
        self.geoshapecoordinates = []
        self.demographics = demographics.Demographics()
        self.economy = economy.Economy()
        self.housing = housing.Housing()

    def convert_to_dict(self):
        self.demographics.demographicquickfacts = json.loads(json.dumps(self.demographics.demographicquickfacts, default=lambda o: o.__dict__))
        self.demographics.highesteducation = json.loads(json.dumps(self.demographics.highesteducation, default=lambda o: o.__dict__))
        self.demographics.race = json.loads(json.dumps(self.demographics.race, default=lambda o: o.__dict__))
        self.demographics.agegroups = json.loads(json.dumps( self.demographics.agegroups, default=lambda o: o.__dict__))
        self.demographics.populationtrends = json.loads(json.dumps(self.demographics.populationtrends, default=lambda o: o.__dict__))
        self.demographics.oneyeargrowth = json.loads(json.dumps(self.demographics.oneyeargrowth, default=lambda o: o.__dict__))
        self.demographics.familytype = json.loads(json.dumps(self.demographics.familytype, default=lambda o: o.__dict__))

        self.economy.medianhouseholdincome = json.loads(json.dumps(self.economy.medianhouseholdincome, default=lambda o: o.__dict__))
        self.economy.unemploymentrate = json.loads(json.dumps(self.economy.unemploymentrate, default=lambda o: o.__dict__))
        self.economy.householdincomerange = json.loads(json.dumps(self.economy.householdincomerange, default=lambda o: o.__dict__))
        self.economy.employmentindustries = json.loads(json.dumps(self.economy.employmentindustries, default=lambda o: o.__dict__))
        self.economy.leadingemploymentindustries = json.loads(json.dumps(self.economy.leadingemploymentindustries, default=lambda o: o.__dict__))
        self.economy.vehiclesowned = json.loads(json.dumps(self.economy.vehiclesowned, default=lambda o: o.__dict__))
        self.economy.meansoftransportation = json.loads(json.dumps(self.economy.meansoftransportation, default=lambda o: o.__dict__))
        self.economy.commutetowork = json.loads(json.dumps(self.economy.commutetowork, default=lambda o: o.__dict__))

        self.housing.housingquickfacts = json.loads(json.dumps( self.housing.housingquickfacts, default=lambda o: o.__dict__))
        self.housing.occupancyrate = json.loads(json.dumps(self.housing.occupancyrate, default=lambda o: o.__dict__))
        self.housing.utilitiesincluded = json.loads(json.dumps(self.housing.utilitiesincluded, default=lambda o: o.__dict__))
        self.housing.housingunitgrowth = json.loads(json.dumps(self.housing.housingunitgrowth, default=lambda o: o.__dict__))
        self.housing.propertytypes = json.loads(json.dumps(self.housing.propertytypes, default=lambda o: o.__dict__))
        self.housing.yearbuilt = json.loads(json.dumps(self.housing.yearbuilt, default=lambda o: o.__dict__))
        self.housing.numberofbedrooms = json.loads(json.dumps(self.housing.numberofbedrooms, default=lambda o: o.__dict__))
        self.housing.yearmovedin = json.loads(json.dumps(self.housing.yearmovedin, default=lambda o: o.__dict__))
        self.housing.incomehousingcost = json.loads(json.dumps(self.housing.incomehousingcost, default=lambda o: o.__dict__))

        self.demographics = json.loads(json.dumps(self.demographics, default=lambda o: o.__dict__))
        self.economy = json.loads(json.dumps(self.economy, default=lambda o: o.__dict__))
        self.housing = json.loads(json.dumps(self.housing, default=lambda o: o.__dict__))

        return self

