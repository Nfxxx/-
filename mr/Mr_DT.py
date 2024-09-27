from cr_monitor.mr.Mr_DTC import MrDTC
from cr_monitor.position.Position_DT import PositionDT
import numpy as np

class MrDT(MrDTC):
    
    def __init__(self, position: PositionDT):
        self.position = position
        self.ccy = "BTC"
        self.assumed_coins = {"BTC"}
        self.short = {}
        self.num_range = range(30, 110, 10)
        self.price_range = np.arange(0.3, 2, 0.1)
        self.mul_range = np.arange(1, 3, 0.2)