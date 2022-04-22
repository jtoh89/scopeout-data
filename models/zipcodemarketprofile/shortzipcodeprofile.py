from models.shortneighborhoorprofile import shortdemographics
from models.shortneighborhoorprofile import shorteconomy
from models.shortneighborhoorprofile import shorthousing
import json
from enums import ModelsDataTypes, ModelsChartTypes

class ShortZipcodeProfile:
    def __init__(self):
        self.geoid = ""
        self.cbsacode = ""
        self.dates = []
        self.mediansaleprice = MedianSalePrice()
        self.mediansalepricemom = MedianSalePriceMom()
        self.dom = Dom()
        self.redfinupdatedate = ""
        self.zillowupdatedate = ""

    def convert_to_dict(self):
        self.mediansaleprice = json.loads(json.dumps(self.mediansaleprice, default=lambda o: o.__dict__))
        self.mediansalepriceMom = json.loads(json.dumps(self.mediansalepriceMom, default=lambda o: o.__dict__))
        self.dom = json.loads(json.dumps(self.dom, default=lambda o: o.__dict__))


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