from enums import ModelsDataTypes, ModelsChartTypes

class Demographics:
    def __init__(self):
        # self.demographicquickfacts = DemographicQuickFacts()
        # self.highesteducation = HighestEducation()
        # self.race = Race()
        # self.agegroups = AgeGroups()
        # self.populationtrends = PopulationTrends()
        self.oneyeargrowth = OneYearGrowth()
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
        self.datatype = ModelsDataTypes.PERCENT.value
        self.charttype = ModelsChartTypes.VERTICAL_BAR.value
        self.labels = []
        self.data = []
        self.colors = []

class PopulationTrends:
    def __init__(self):
        self.title = "Population Trends"
        self.datatype = ModelsDataTypes.INTEGER.value
        self.charttype = ModelsChartTypes.LINE.value
        self.data1Name = "Population"
        self.data1 = []
        self.labels1 = []
        self.data2Name = "Growth Rate"
        self.data2 = []
        self.labels2 = []

class OneYearGrowth:
    def __init__(self):
        self.hascolors = True
        self.title = "1 Year Population Growth"
        self.datatype = ModelsDataTypes.PERCENT.value
        self.charttype = ModelsChartTypes.HORIZONTAL_BAR.value
        self.labels = []
        self.data = []
        self.colors = []
        self.hasData = True

class FamilyType:
    def __init__(self):
        self.title = "Family Type"
        self.datatype = ModelsDataTypes.PERCENT.value
        self.charttype = ModelsChartTypes.DONUT.value
        self.labels = []
        self.data = []
        self.colors = []
