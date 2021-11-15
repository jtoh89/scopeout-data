class Housing:
    def __init__(self):
        self.housingquickfacts = HousingQuickFacts()
        self.occupancyrate = OccupancyRate()
        self.utilitiesincluded = UtilitiesIncluded()
        self.housingunitgrowth = HousingUnitGrowth()
        self.propertytypes = PropertyTypes()
        self.yearbuilt = YearBuilt()
        self.numberofbedrooms = NumberOfBedrooms()
        self.yearmovedin = YearMovedIn()
        self.incomehousingcost = IncomeHousingCost()


class HousingQuickFacts:
    def __init__(self):
        self.title = "Quick Facts",
        self.charttype = "summary",
        self.label1 = "5 Year Homeowner Growth",
        self.value1 = "",
        self.label2 = "5 Year Renter Household Growth",
        self.value2 = "",
        self.label3 = "5 Year Housing Unit Growth",
        self.value3 = "",
        self.label4 = "Dominant Housing Type",
        self.value4 = ""



class OccupancyRate:
    def __init__(self):
        self.hascolors = True,
        self.title = "Occupancy rate"
        self.charttype = "donut"
        self.labels = []
        self.data = []
        self.colors = []
        self.datatype = "PERCENT"


class UtilitiesIncluded:
    def __init__(self):
        self.hascolors = True,
        self.title = "Utilities in Rent"
        self.charttype = "donut"
        self.labels = []
        self.data = []
        self.colors = []
        self.datatype = "PERCENT"


class HousingUnitGrowth:
    def __init__(self):
        self.title = "Housing Unit Growth"
        self.charttype = "line"
        self.data1Name = "# of Housing Units"
        self.labels1 = []
        self.data1 = []
        self.data2name = "Growth Rate"
        self.labels2 = []
        self.data2 = []


class PropertyTypes:
    def __init__(self):
        self.title = "Property Types"
        self.charttype = "piecharttoggle"
        self.labels = []
        self.colors = []
        self.data1Name = "All"
        self.data1 = []
        self.data2name = "Owners"
        self.data2 = []
        self.data3name = "Renters"
        self.data3 = []
        self.datatype = "PERCENT"


class YearBuilt:
    def __init__(self):
        self.hascolors = True
        self.title = "Year Built"
        self.charttype = "verticalbar"
        self.labels = []
        self.data = []
        self.datatype = "PERCENT"

class NumberOfBedrooms:
    def __init__(self):
        self.title = "Number of Bedrooms"
        self.charttype = "piecharttoggle"
        self.labels = []
        self.colors = []
        self.data1Name = "All"
        self.data1 = []
        self.data2name = "Owners"
        self.data2 = []
        self.data3name = "Renters"
        self.data3 = []
        self.datatype = "PERCENT"



class YearMovedIn:
    def __init__(self):
        self.title = "Year Moved In"
        self.charttype = "piecharttoggle"
        self.labels = []
        self.colors = []
        self.data1Name = "All"
        self.data1 = []
        self.data2name = "Owners"
        self.data2 = []
        self.data3name = "Renters"
        self.data3 = []
        self.datatype = "PERCENT"



class IncomeHousingCost:
    def __init__(self):
        self.title = "% Income on Housing Costs"
        self.charttype = "piecharttoggle"
        self.labels = []
        self.colors = []
        self.data1Name = "All"
        self.data1 = []
        self.data2name = "Owners"
        self.data2 = []
        self.data3name = "Renters"
        self.data3 = []
        self.datatype = "PERCENT"



