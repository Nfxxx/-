from cr_monitor.mr.Mr_SSFO import MrSSFO
from cr_monitor.position.Position_DTC import PositionDTC
import numpy as np
import copy

class MrDTC(MrSSFO):
    
    def __init__(self, position: PositionDTC):
        self.position = position
        self.ccy = "BTC"
        self.num_range = range(30, 110, 10)
        self.price_range = np.arange(0.3, 2, 0.1)
        self.mul_range = np.arange(1, 3, 0.1)
        self.assumed_coins = {"BTC", "ETH"}
        self.short = {"BTC", "ETH"}
    
    def run_assumed_open(self):
        self.position.amount_master = {coin: 0 for coin in self.assumed_coins}
        ccy_price = self.position.get_master_price(coin = self.ccy)
        now_price = self.position.get_now_price_master()
        self.position.price_master = now_price
        self.position.price_slave = copy.deepcopy(self.position.price_master)
        coins_number = len(self.assumed_coins)
        assumed_open:dict[float, dict[float, dict[float, float]]] = {}
        detail_open: dict[float, dict[float, dict[float, dict[str, float]]]] = {}
        for num in self.num_range:
            ret = {}
            all_ret = {}
            for mul in self.mul_range:
                mul = round(mul, 2)
                single_mv = mul / coins_number * num * ccy_price
                self.position.equity = {self.ccy: num}
                self.position.adjEq = num * ccy_price
                self.position.start_adjEq = num * ccy_price
                self.position.liability = 0
                assumed_holding = {}
                for coin in self.assumed_coins:
                    size = self.position.contract_master[coin] if coin in self.position.contract_master.keys() else self.position.get_contractsize_master(coin)
                    assumed_holding[coin] = - single_mv / size if coin in self.short else single_mv / size
                self.position.amount_master = copy.deepcopy(assumed_holding)
                self.position.amount_slave = {coin: single_mv / now_price[coin] if coin in self.short else - single_mv / now_price[coin] for coin in self.assumed_coins}
                self.position.now_price_master = copy.deepcopy(self.position.price_master)
                self.position.now_price_slave = copy.deepcopy(self.position.price_slave)
                result, all_result = self.run_price_influence()
                ret[mul] = copy.deepcopy(result)
                all_ret[mul] = copy.deepcopy(all_result)
            assumed_open[num] = copy.deepcopy(ret)
            detail_open[num] = copy.deepcopy(all_ret)
        self.assumed_open = assumed_open
        self.detail_open = detail_open
        return assumed_open