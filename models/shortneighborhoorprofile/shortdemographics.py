from enums import ModelsDataTypes, ModelsChartTypes
from models.generics import OneLineChart

class Demographics:
    def __init__(self):
        # self.demographicquickfacts = DemographicQuickFacts()
        # self.highesteducation = HighestEducation()
        self.race = Race()
        self.agegroups = AgeGroups()
        self.populationhistorical = OneLineChart(title="Total Population", datatype=ModelsDataTypes.INTEGER.value)
        # self.oneyeargrowth = OneYearGrowth()
        # self.familytype = FamilyType()


class DemographicQuickFacts:
    def __init__(self):
        self.title = "Quick Facts"
        self.charttype = ModelsChartTypes.SUMMARY.value
        self.label1 = "Poverty rate"
        self.value1 = ""
        self.label2 = "% of Children"
        self.value2 = ""
        self.label3 = "% of College Students"
        self.value3 = ""
        self.label4 = "% of Veterans"
        self.value4 = ""


class HighestEducation:
    def __init__(self):
        self.title = "Highest Education"
        self.datatype = ModelsDataTypes.PERCENT.value
        self.charttype = ModelsChartTypes.DONUT.value
        self.labels = []
        self.data = []
        self.colors = []

class Race:
    def __init__(self):
        self.title = "Race/Ethnicity"
        self.datatype = ModelsDataTypes.PERCENT.value
        self.charttype = ModelsChartTypes.DONUT.value
        self.labels = []
        self.data = []
        self.colors = []

class AgeGroups:
    def __init__(self):
        self.title = "Age Groups"
        self.hascolors = True
        self.datatype = ModelsDataTypes.PERCENT.value
        self.charttype = ModelsChartTypes.VERTICAL_BAR.value
        self.labels = []
        self.data = []
        self.colors = []

class FamilyType:
    def __init__(self):
        self.title = "Family Type"
        self.datatype = ModelsDataTypes.PERCENT.value
        self.charttype = ModelsChartTypes.DONUT.value
        self.labels = []
        self.data = []
        self.colors = []
