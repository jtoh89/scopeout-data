import json
from enums import ModelsChartTypes, ModelsDataTypes
from models.generics import OneLineChart, TwoLineChart

class CbsaMarketProfile:
    def __init__(self):
        self.cbsacode = ""
        self.cbsaname = ""
        self.urlslug = ""
        self.mediansaleprice = OneLineChart(title="Median Sale Price", datatype=ModelsDataTypes.DOLLAR.value)
        self.mediansalepricemom = OneLineChart(title="Median Sale Price MoM Change", datatype=ModelsDataTypes.PERCENT.value)
        self.medianppsf = OneLineChart(title="Median Price Per SqFt", datatype=ModelsDataTypes.DOLLAR.value)
        self.monthsofsupply = OneLineChart(title="Months Of Supply", datatype=ModelsDataTypes.FLOAT.value)
        self.mediandom = OneLineChart(title="Median Days On Market", datatype=ModelsDataTypes.INTEGER.value)
        self.pricedrops = OneLineChart(title="% of Listings With Price Drops", datatype=ModelsDataTypes.PERCENT.value)
        self.rentaltrends = OneLineChart(title="Median Rent", datatype=ModelsDataTypes.DOLLAR.value)
        self.unemploymentrate = OneLineChart(title="Unemployment Rate", datatype=ModelsDataTypes.PERCENT.value)
        self.buildingpermits = OneLineChart(title="Building Permits", datatype=ModelsDataTypes.INTEGER.value)
        self.housingunitsvshouseholds = TwoLineChart(title="Housing Units vs Households", datatype=ModelsDataTypes.INTEGER.value)
        self.totalpopulationgrowth = OneLineChart(title="Total Population Growth", datatype=ModelsDataTypes.INTEGER.value)


    def convert_to_dict(self):
        self.mediansaleprice = json.loads(json.dumps(self.mediansaleprice, default=lambda o: o.__dict__))
        self.mediansalepricemom = json.loads(json.dumps(self.mediansalepricemom, default=lambda o: o.__dict__))
        self.medianppsf = json.loads(json.dumps(self.medianppsf, default=lambda o: o.__dict__))
        self.monthsofsupply = json.loads(json.dumps(self.monthsofsupply, default=lambda o: o.__dict__))
        self.mediandom = json.loads(json.dumps(self.mediandom, default=lambda o: o.__dict__))
        self.pricedrops = json.loads(json.dumps(self.pricedrops, default=lambda o: o.__dict__))
        self.rentaltrends = json.loads(json.dumps(self.rentaltrends, default=lambda o: o.__dict__))
        self.unemploymentrate = json.loads(json.dumps(self.unemploymentrate, default=lambda o: o.__dict__))
        self.buildingpermits = json.loads(json.dumps(self.buildingpermits, default=lambda o: o.__dict__))
        self.totalpopulationgrowth = json.loads(json.dumps(self.totalpopulationgrowth, default=lambda o: o.__dict__))
        self.housingunitsvshouseholds = json.loads(json.dumps(self.housingunitsvshouseholds, default=lambda o: o.__dict__))

        return self
