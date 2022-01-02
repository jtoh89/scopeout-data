
class Demographics:
    def __init__(self):
        self.demographicquickfacts = DemographicQuickFacts()
        self.highesteducation = HighestEducation()
        self.race = Race()
        self.agegroups = AgeGroups()
        self.populationtrends = PopulationTrends()
        self.oneyeargrowth = OneYearGrowth()
        self.familytype = FamilyType()


class DemographicQuickFacts:
    def __init__(self):
        self.title = "Quick Facts"
        self.charttype = "summary"
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
        self.datatype = "PERCENT"
        self.charttype = "donut"
        self.labels = []
        self.data = []
        self.colors = []

class Race:
    def __init__(self):
        self.title = "Race/Ethnicity"
        self.datatype = "PERCENT"
        self.charttype = "donut"
        self.labels = []
        self.data = []
        self.colors = []

class AgeGroups:
    def __init__(self):
        self.title = "Age Groups"
        self.datatype = "PERCENT"
        self.charttype = "verticalbar"
        self.labels = []
        self.data = []
        self.colors = []

class PopulationTrends:
    def __init__(self):
        self.title = "Population Trends"
        self.datatype = "INTEGER"
        self.charttype = "line"
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
        self.datatype = "PERCENT"
        self.charttype = "horizontalbar"
        self.labels = []
        self.data = []
        self.colors = []

class FamilyType:
    def __init__(self):
        self.title = "Family Type"
        self.datatype = "PERCENT"
        self.charttype = "donut"
        self.labels = []
        self.data = []
        self.colors = []
