import json

class CbsaMarketProfile:
    def __init__(self):
        self.cbsacode = ""
        self.cbsaname = ""
        self.urlslug = ""
        self.mediansaleprice = MedianSalePrice()
        self.medianppsf = Median_PPSF()
        self.monthsofsupply = MonthsOfSupply()
        self.mediandom = MedianDOM()
        self.pricedrops = PriceDrops()
        self.rentaltrends = OneLineChart(title="Median Rent", datatype="DOLLAR")
        self.unemploymentrate = OneLineChart(title="Unemployment Rate", datatype="PERCENT")
        self.buildingpermits = BuildingPermits()
        self.totalhousingunit = HousingUnitGrowth()
        self.totalpopulationgrowth = PopulationGrowth()


    def convert_to_dict(self):
        self.mediansaleprice = json.loads(json.dumps(self.mediansaleprice, default=lambda o: o.__dict__))
        self.medianppsf = json.loads(json.dumps(self.medianppsf, default=lambda o: o.__dict__))
        self.monthsofsupply = json.loads(json.dumps(self.monthsofsupply, default=lambda o: o.__dict__))
        self.mediandom = json.loads(json.dumps(self.mediandom, default=lambda o: o.__dict__))
        self.pricedrops = json.loads(json.dumps(self.pricedrops, default=lambda o: o.__dict__))
        self.rentaltrends = json.loads(json.dumps(self.rentaltrends, default=lambda o: o.__dict__))
        self.unemploymentrate = json.loads(json.dumps(self.unemploymentrate, default=lambda o: o.__dict__))
        self.buildingpermits = json.loads(json.dumps(self.buildingpermits, default=lambda o: o.__dict__))
        self.totalpopulationgrowth = json.loads(json.dumps(self.totalpopulationgrowth, default=lambda o: o.__dict__))
        self.totalhousingunit = json.loads(json.dumps(self.totalhousingunit, default=lambda o: o.__dict__))

        return self


class PopulationGrowth:
    def __init__(self):
        self.title = "Total Population Growth"
        self.datatype = ModelsDataTypes.INTEGER.value
        self.labels = []
        self.dataName = ""
        self.data = []

class HousingUnitGrowth:
    def __init__(self):
        self.title = "Total Housing Unit Growth"
        self.datatype = ModelsDataTypes.INTEGER.value
        self.labels = []
        self.dataName = ""
        self.data = []

class BuildingPermits:
    def __init__(self):
        self.title = "Building Permits"
        self.datatype = ModelsDataTypes.INTEGER.value
        self.labels = []
        self.dataName = ""
        self.data = []


class UnemploymentData:
    def __init__(self):
        self.title = "Unemployment Rate"
        self.charttype = ModelsChartTypes.LINE.value
        self.labels = []
        self.dataName = ""
        self.data = []
        self.datatype = ModelsDataTypes.PERCENT.value

class OneLineChart():
    def __init__(self, title, datatype):
        self.title = title
        self.charttype = ModelsChartTypes.LINE.value
        self.labels = []
        self.dataName = ""
        self.data = []
        self.datatype = datatype

class PropertyTypeDataObject:
    def __init__(self, title, charttype):
        self.title = title
        self.charttype = charttype
        self.labels = []
        self.data1Name = ""
        self.data1 = []
        self.data2Name = ""
        self.data2 = []
        self.data3Name = ""
        self.data3 = []


class MedianSalePrice:
    def __init__(self):
        self.title = "Median Sale Price"
        self.datatype = ModelsDataTypes.DOLLAR.value
        self.labels = []
        self.data1Name = "All"
        self.data1 = []
        self.data2Name = "Single Family"
        self.data2 = []
        self.data3Name = "2-4 Units"
        self.data3 = []

class Median_PPSF:
    def __init__(self):
        self.title = "Median Price Per SqFt"
        self.datatype =  ModelsDataTypes.DOLLAR.value
        self.labels = []
        self.data1Name = "All"
        self.data1 = []
        self.data2Name = "Single Family"
        self.data2 = []
        self.data3Name = "2-4 Units"
        self.data3 = []


class MonthsOfSupply:
    def __init__(self):
        self.title = "Months Of Supply"
        self.datatype = ModelsDataTypes.FLOAT.value
        self.labels = []
        self.data1Name = "All"
        self.data1 = []
        self.data2Name = "Single Family"
        self.data2 = []
        self.data3Name = "2-4 Units"
        self.data3 = []


class MedianDOM:
    def __init__(self):
        self.title = "Median Days On Market"
        self.datatype = ModelsDataTypes.INTEGER.value
        self.labels = []
        self.data1Name = "All"
        self.data1 = []
        self.data2Name = "Single Family"
        self.data2 = []
        self.data3Name = "2-4 Units"
        self.data3 = []


class PriceDrops:
    def __init__(self):
        self.title = "% of Listings With Price Drops"
        self.datatype = ModelsDataTypes.PERCENT.value
        self.labels = []
        self.data1Name = "All"
        self.data1 = []
        self.data2Name = "Single Family"
        self.data2 = []
        self.data3Name = "2-4 Units"
        self.data3 = []
