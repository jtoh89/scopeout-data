from enums import ModelsDataTypes, ModelsChartTypes
class Economy:
    def __init__(self):
        self.medianhouseholdincome = MedianHouseholdIncome()
        self.medianhouseholdincomehistorical = MedianHouseholdIncomeHistorical()
        self.unemploymentrate = UnemploymentRate()
        # self.householdincomerange = HouseholdIncomeRange()
        # self.employmentindustries = EmploymentIndustries()
        # self.leadingemploymentindustries = LeadingEmploymentIndustries()
        # self.vehiclesowned = VehiclesOwned()
        # self.meansoftransportation = MeansOfTransportation()
        # self.commutetowork = CommuteToWork()


class MedianHouseholdIncome:
    def __init__(self):
        self.hascolors = True
        self.title = "Median Household Income"
        self.charttype = ModelsChartTypes.VERTICAL_BAR_TOGGLE.value
        self.labels = []
        self.data1Name = "All"
        self.data1 = []
        self.data2Name = "Owners"
        self.data2 = []
        self.data3Name = "Renters"
        self.data3 = []
        self.datatype = ModelsDataTypes.DOLLAR.value


class MedianHouseholdIncomeHistorical:
    def __init__(self):
        self.hascolors = True
        self.title = "Median Household Income Trends"
        self.charttype = ModelsChartTypes.VERTICAL_BAR_TOGGLE.value
        self.labels = []
        self.data1Name = "All"
        self.data1 = []
        self.data2Name = "Owners"
        self.data2 = []
        self.data3Name = "Renters"
        self.data3 = []
        self.datatype = ModelsDataTypes.DOLLAR.value

class UnemploymentRate:
    def __init__(self):
        self.hascolors = True
        self.title = "Unemployment Rate"
        self.datatype = ModelsDataTypes.PERCENT.value
        self.charttype = ModelsChartTypes.HORIZONTAL_BAR.value
        self.labels = []
        self.data = []
        self.colors = []


class HouseholdIncomeRange:
    def __init__(self):
        self.title = "Household Income Range"
        self.datatype = ModelsDataTypes.PERCENT.value
        self.charttype = ModelsChartTypes.VERTICAL_BAR_TOGGLE.value
        self.labels = []
        self.data1Name = "All"
        self.data1 = []
        self.data2Name = "Owners"
        self.data2 = []
        self.data3Name = "Renters"
        self.data3 = []


class EmploymentIndustries:
    def __init__(self):
        self.hascolors = True
        self.title = "Employment Industries"
        self.datatype = ModelsDataTypes.PERCENT.value
        self.charttype = ModelsChartTypes.VERTICAL_BAR.value
        self.labels = []
        self.data = []
        self.colors = []


class LeadingEmploymentIndustries:
    def __init__(self):
        self.title = "Leading Employment Industries"
        self.charttype = ModelsChartTypes.TABLE.value
        self.data = []


class VehiclesOwned:
    def __init__(self):
        self.title = "Vehicles Owned"
        self.datatype = ModelsDataTypes.PERCENT.value
        self.charttype = ModelsChartTypes.PIE_CHART_TOGGLE.value
        self.labels = []
        self.colors = []
        self.data1Name = "All"
        self.data1 = []
        self.data2Name = "Owners"
        self.data2 = []
        self.data3Name = "Renters"
        self.data3 = []


class MeansOfTransportation:
    def __init__(self):
        self.hascolors = True,
        self.title = "Means of Transportation"
        self.datatype = ModelsDataTypes.PERCENT.value
        self.charttype = ModelsChartTypes.DONUT.value
        self.labels = []
        self.data = []
        self.colors = []

class CommuteToWork:
    def __init__(self):
        self.hascolors = True
        self.title = "Commute to Work"
        self.datatype = ModelsDataTypes.PERCENT.value
        self.charttype = ModelsChartTypes.HORIZONTAL_BAR.value
        self.labels = []
        self.data = []
        self.colors = []

