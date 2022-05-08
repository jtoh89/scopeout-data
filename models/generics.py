from enums import ModelsChartTypes

class OneLineChart():
    def __init__(self, title, datatype):
        self.title = title
        self.charttype = ModelsChartTypes.LINE.value
        self.datatype = datatype
        self.labels = []
        self.dataName = title
        self.data = []

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


class ComparisonBarChart():
    def __init__(self, title, datatype):
        self.title = title
        self.charttype = ModelsChartTypes.VERTICAL_COMPARISON_BAR.value
        self.datatype = datatype
        self.colors: []
        self.labels = []
        self.datasets = []
