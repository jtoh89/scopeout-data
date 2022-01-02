import json

class CbsaMarketProfile:
    def __init__(self):
        self.cbsacode = ""
        self.mediansaleprice = MedianSalePrice()
        self.medianppsf = Median_PPSF()
        self.monthsofsupply = MonthsOfSupply()
        self.mediandom = MedianDOM()
        self.pricedrops = PriceDrops()
        self.rentaltrends = OneLineChart(title="Median Rent", datatype="DOLLAR")
        self.unemploymentdata = OneLineChart(title="Unemployment Rate", datatype="PERCENT")
        self.buildingpermits = BuildingPermits()

    def convert_to_dict(self):
        self.mediansaleprice = json.loads(json.dumps(self.mediansaleprice, default=lambda o: o.__dict__))
        self.medianppsf = json.loads(json.dumps(self.medianppsf, default=lambda o: o.__dict__))
        self.monthsofsupply = json.loads(json.dumps(self.monthsofsupply, default=lambda o: o.__dict__))
        self.mediandom = json.loads(json.dumps(self.mediandom, default=lambda o: o.__dict__))
        self.pricedrops = json.loads(json.dumps(self.pricedrops, default=lambda o: o.__dict__))
        self.rentaltrends = json.loads(json.dumps(self.rentaltrends, default=lambda o: o.__dict__))
        self.unemploymentdata = json.loads(json.dumps(self.unemploymentdata, default=lambda o: o.__dict__))

        self.buildingpermits.units_all = json.loads(json.dumps(self.buildingpermits.units_all, default=lambda o: o.__dict__))
        self.buildingpermits.units_1 = json.loads(json.dumps(self.buildingpermits.units_1, default=lambda o: o.__dict__))
        self.buildingpermits.units_2_to_4 = json.loads(json.dumps(self.buildingpermits.units_2_to_4, default=lambda o: o.__dict__))
        self.buildingpermits.units_5plus = json.loads(json.dumps(self.buildingpermits.units_5plus, default=lambda o: o.__dict__))

        self.buildingpermits = json.loads(json.dumps(self.buildingpermits, default=lambda o: o.__dict__))

        return self


class BuildingPermits:
    def __init__(self):
        self.title = "Building Permits"
        self.datatype = "INTEGER"
        self.labels = []
        self.units_all = BuildingPermitUnitData(dataname="Total")
        self.units_1 = BuildingPermitUnitData(dataname="1 unit")
        self.units_2_to_4 = BuildingPermitUnitData(dataname="2-4 Units")
        self.units_5plus = BuildingPermitUnitData(dataname="5+ Units")

class BuildingPermitUnitData:
    def __init__(self, dataname):
        self.dataName = dataname
        self.data = []


class UnemploymentData:
    def __init__(self):
        self.title = "Unemployment Rate"
        self.charttype = "line"
        self.labels = []
        self.dataName = ""
        self.data = []
        self.datatype = "PERCENT"

class OneLineChart():
    def __init__(self, title, datatype):
        self.title = title
        self.charttype = "line"
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
        self.datatype = "DOLLAR"
        self.labels = []
        self.data1Name = "All"
        self.data1 = []
        self.data2Name = "Single Family"
        self.data2 = []
        self.data3Name = "Multi-Family"
        self.data3 = []

class Median_PPSF:
    def __init__(self):
        self.title = "Median Price Per SqFt"
        self.datatype = "INTEGER"
        self.labels = []
        self.data1Name = "All"
        self.data1 = []
        self.data2Name = "Single Family"
        self.data2 = []
        self.data3Name = "Multi-Family"
        self.data3 = []


class MonthsOfSupply:
    def __init__(self):
        self.title = "Months Of Supply"
        self.datatype = "FLOAT"
        self.labels = []
        self.data1Name = "All"
        self.data1 = []
        self.data2Name = "Single Family"
        self.data2 = []
        self.data3Name = "Multi-Family"
        self.data3 = []


class MedianDOM:
    def __init__(self):
        self.title = "Median Days On Market"
        self.datatype = "INTEGER"
        self.labels = []
        self.data1Name = "All"
        self.data1 = []
        self.data2Name = "Single Family"
        self.data2 = []
        self.data3Name = "Multi-Family"
        self.data3 = []


class PriceDrops:
    def __init__(self):
        self.title = "% of Listings With Price Drops"
        self.datatype = "PERCENT"
        self.labels = []
        self.data1Name = "All"
        self.data1 = []
        self.data2Name = "Single Family"
        self.data2 = []
        self.data3Name = "Multi-Family"
        self.data3 = []
