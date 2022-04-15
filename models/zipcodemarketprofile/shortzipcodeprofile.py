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
        self.mediansalepriceMom = MedianSalePriceMom()
        self.dom = Dom()

    def convert_to_dict(self):
        self.mediansaleprice = json.loads(json.dumps(self.mediansaleprice, default=lambda o: o.__dict__))
        self.mediansalepriceMom = json.loads(json.dumps(self.mediansalepriceMom, default=lambda o: o.__dict__))
        self.dom = json.loads(json.dumps(self.dom, default=lambda o: o.__dict__))


class MedianSalePrice:
    def __init__(self):
        self.title = "Median Sale Price"
        self.datatype = ModelsDataTypes.DOLLAR.value
        self.labels = []
        self.data1Name = ""
        self.data1 = []
        self.data2Name = ""
        self.data2 = []

class MedianSalePriceMom:
    def __init__(self):
        self.title = "Median Sale Price MoM Change"
        self.datatype = ModelsDataTypes.PERCENT.value
        self.labels = []
        self.data1Name = ""
        self.data1 = []
        self.data2Name = ""
        self.data2 = []

class Dom:
    def __init__(self):
        self.title = "Days on Market"
        self.datatype = ModelsDataTypes.INTEGER.value
        self.labels = []
        self.data1Name = ""
        self.data1 = []
        self.data2Name = ""
        self.data2 = []
