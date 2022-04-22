from models.shortneighborhoorprofile import shortdemographics
from models.shortneighborhoorprofile import shorteconomy
from models.shortneighborhoorprofile import shorthousing
import json
from enums import ModelsDataTypes, ModelsChartTypes

class ShortZipcodeProfile:
    def __init__(self):
        self.geoid = ""
        self.cbsacode = ""
        self.mediansaleprice = MedianSalePrice()
        self.mediansalepricemom = MedianSalePriceMom()
        self.dom = Dom()
        self.rentaltrends = TwoLineChart(title="Median Rent", datatype=ModelsDataTypes.DOLLAR.value)
        self.redfinupdatedate = ""
        self.zillowupdatedate = ""

    def convert_to_dict(self):
        self.mediansaleprice = json.loads(json.dumps(self.mediansaleprice, default=lambda o: o.__dict__))
        self.mediansalepricemom = json.loads(json.dumps(self.mediansalepricemom, default=lambda o: o.__dict__))
        self.dom = json.loads(json.dumps(self.dom, default=lambda o: o.__dict__))
        self.rentaltrends = json.loads(json.dumps(self.rentaltrends, default=lambda o: o.__dict__))

        return self


class MedianSalePrice:
    def __init__(self):
        self.title = "Median Sale Price"
        self.datatype = ModelsDataTypes.DOLLAR.value
        self.hascolors = True
        self.charttype = ModelsChartTypes.HORIZONTAL_BAR.value
        self.labels = []
        self.data = []
        self.colors = []

class MedianSalePriceMom:
    def __init__(self):
        self.title = "Median Sale Price MoM Change"
        self.datatype = ModelsDataTypes.PERCENT.value
        self.hascolors = True
        self.charttype = ModelsChartTypes.HORIZONTAL_BAR.value
        self.labels = []
        self.data = []
        self.colors = []

class Dom:
    def __init__(self):
        self.title = "Days on Market"
        self.hascolors = True
        self.charttype = ModelsChartTypes.HORIZONTAL_BAR.value
        self.labels = []
        self.data = []
        self.colors = []


class RentalTrends:
    def __init__(self):
        self.title = "Median Rent"
        self.charttype = ModelsChartTypes.LINE.value
        self.datatype = ModelsDataTypes.INTEGER.value
        self.data1Name = "# of Housing Units"
        self.labels1 = []
        self.data1 = []
        self.data2Name = "Growth Rate"
        self.labels2 = []
        self.data2 = []

        self.labels = []
        self.data = []
        self.colors = []

class TwoLineChart():
    def __init__(self, title, datatype):
        self.title = title
        self.charttype = ModelsChartTypes.MULTI_LINE.value
        self.datatype = datatype
        self.labels = []
        self.data1Name = ""
        self.data1 = []
        self.data2Name = ""
        self.data2 = []