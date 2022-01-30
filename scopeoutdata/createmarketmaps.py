class NeighborhoodMarketProfile:
    def __init__(self):
        self.data = UnemploymentData()


class UnemploymentData:
    def __init__(self):
        self.title = "Unemployment Rate"
        self.charttype = "line"
        self.labels = []
        self.data1Name = ""
        self.data1 = []
        self.data2Name = "United States"
        self.data2 = []
        self.datatype = "DOLLAR"
        self.hasData = True

