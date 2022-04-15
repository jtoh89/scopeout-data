from enums import ModelsDataTypes, ModelsChartTypes


class NeighborhoodMarketProfile:
    def __init__(self):
        self.mediansaleprice = MedianSalePrice()
        self.medianppsf = Median_PPSF()
        self.monthsofsupply = MonthsOfSupply()
        self.mediandom = MedianDOM()
        self.pricedrops = PriceDrops()
        self.rentaltrends = RentalTrends()
        self.unemploymentrate = UnemploymentRate()


class UnemploymentRate:
    def __init__(self):
        self.title = "Unemployment Rate"
        self.charttype = ModelsChartTypes.LINE.value
        self.labels = []
        self.data1Name = ""
        self.data1 = []
        self.data2Name = "United States"
        self.data2 = []
        self.datatype = ModelsDataTypes.DOLLAR.value
        self.hasData = True


class RentalTrends:
    def __init__(self):
        self.title = "Median Rent"
        self.charttype = ModelsChartTypes.LINE.value
        self.labels = []
        self.data1Name = ""
        self.data1 = []
        self.data2Name = "United States"
        self.data2 = []
        self.datatype = ModelsDataTypes.DOLLAR.value
        self.hasData = True



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
        self.all = PropertyTypeDataObject(title="All", charttype="line")
        self.singlefamily = PropertyTypeDataObject(title="Single Family", charttype="line")
        self.multifamily = PropertyTypeDataObject(title="2-4 Units", charttype="line")
        self.hasData = True

class Median_PPSF:
    def __init__(self):
        self.title = "Median Price Per SqFt"
        self.datatype = ModelsDataTypes.INTEGER.value
        self.all = PropertyTypeDataObject(title="All", charttype="line")
        self.singlefamily = PropertyTypeDataObject(title="Single Family", charttype="line")
        self.multifamily = PropertyTypeDataObject(title="2-4 Units", charttype="line")
        self.hasData = True


class MonthsOfSupply:
    def __init__(self):
        self.title = "Months Of Supply"
        self.datatype = ModelsDataTypes.FLOAT.value
        self.all = PropertyTypeDataObject(title="All", charttype="line")
        self.singlefamily = PropertyTypeDataObject(title="Single Family", charttype="line")
        self.multifamily = PropertyTypeDataObject(title="2-4 Units", charttype="line")
        self.hasData = True


class MedianDOM:
    def __init__(self):
        self.title = "Median Days On Market"
        self.datatype = ModelsDataTypes.INTEGER.value
        self.all = PropertyTypeDataObject(title="All", charttype="line")
        self.singlefamily = PropertyTypeDataObject(title="Single Family", charttype="line")
        self.multifamily = PropertyTypeDataObject(title="2-4 Units", charttype="line")
        self.hasData = True


class PriceDrops:
    def __init__(self):
        self.title = "% of Listings With Price Drops"
        self.datatype =  "PERCENT"
        self.all = PropertyTypeDataObject(title="All", charttype="line")
        self.singlefamily = PropertyTypeDataObject(title="Single Family", charttype="line")
        self.multifamily = PropertyTypeDataObject(title="2-4 Units", charttype="line")
        self.hasData = True
        self.datatype = ModelsDataTypes.PERCENT.value
