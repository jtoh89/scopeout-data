
class Economy:
    def __init__(self):
        self.medianhouseholdincome = MedianHouseholdIncome()
        self.unemploymentrate = UnemploymentRate()
        self.householdincomerange = HouseholdIncomeRange()
        self.employmentindustries = EmploymentIndustries()
        self.leadingemploymentindustries = LeadingEmploymentIndustries()
        self.vehiclesowned = VehiclesOwned()
        self.meansoftransportation = MeansOfTransportation()
        self.commutetowork = CommuteToWork()


class MedianHouseholdIncome:
    def __init__(self):
        self.hascolors = True
        self.title = "Median Household Income"
        self.charttype = "verticalbartoggle"
        self.labels = []
        self.data1Name = "All"
        self.data1 = []
        self.data2Name = "Owners"
        self.data2 = []
        self.data3Name = "Renters"
        self.data3 = []
        self.datatype = "DOLLAR"



class UnemploymentRate:
    def __init__(self):
        self.hascolors = True
        self.title = "Unemployment Rate"
        self.charttype = "horizontalbar"
        self.labels = []
        self.data = []
        self.colors = []
        self.datatype = "PERCENT"


class HouseholdIncomeRange:
    def __init__(self):
        self.title = "Household Income Range"
        self.charttype = "verticalbartoggle"
        self.labels = []
        self.data1Name = "All"
        self.data1 = []
        self.data2Name = "Owners"
        self.data2 = []
        self.data3Name = "Renters"
        self.data3 = []
        self.datatype = "PERCENT"


class EmploymentIndustries:
    def __init__(self):
        self.hascolors = True
        self.title = "Employment Industries"
        self.charttype ="verticalbar"
        self.labels = []
        self.data = []
        self.colors = []
        self.datatype = "PERCENT"


class LeadingEmploymentIndustries:
    def __init__(self):
        self.title = "Leading Employment Industries"
        self.charttype = "table"
        self.data = []


class VehiclesOwned:
    def __init__(self):
        self.title = "Vehicles Owned"
        self.charttype = "piecharttoggle"
        self.labels = []
        self.colors = []
        self.data1Name = "All"
        self.data1 = []
        self.data2Name = "Owners"
        self.data2 = []
        self.data3Name = "Renters"
        self.data3 = []
        self.datatype = "PERCENT"


class MeansOfTransportation:
    def __init__(self):
        self.hascolors = True,
        self.title = "Means of Transportation"
        self.charttype = "donut"
        self.labels = []
        self.data = []
        self.colors = []
        self.datatype = "PERCENT"

class CommuteToWork:
    def __init__(self):
        self.hascolors = True
        self.title = "Commute to Work"
        self.charttype = "horizontalbar"
        self.labels = []
        self.data = []
        self.colors = []
        self.datatype = "PERCENT"

