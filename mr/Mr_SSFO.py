from cr_monitor.position.Position_SSFO import PositionSSFO
import copy
import numpy as np
import pandas as pd

class MrSSFO(object):
    """SSFO means spot and usdt_swap in okex."""
    
    def __init__(self, position: PositionSSFO):
        self.position = position
        self.ccy = "BTC"
        self.price_range = np.arange(0.3, 2, 0.1)
        self.num_range = range(30, 110, 10)
        self.mul_range = np.arange(0.5, 1.6, 0.1)
        self.assumed_coins = {"BLUR", "AR", "GFT", "FIL", "ARB", "PEOPLE", "ETH", "SUSHI", "ICP", "THETA"}
        self.short = {}
    
    def run_price_influence(self) -> tuple[dict[float, float], dict[float, dict[str, float]]]:
        result: dict[float, float] = {}
        all_result: dict[float, dict[str, float]] = {}
        adjEq0 = copy.deepcopy(self.position.start_adjEq)
        now_price_master = copy.deepcopy(self.position.now_price_master)
        now_price_slave = copy.deepcopy(self.position.now_price_slave)
        for change in self.price_range:
            detail = {}
            change = round(change, 2)
            self.position.start_adjEq = adjEq0 * change if self.ccy not in ["USDT", "USD", "USDC", "BUSD"] else adjEq0
            self.position.adjEq = adjEq0 * change if self.ccy not in ["USDT", "USD", "USDC", "BUSD"] else adjEq0
            for coin in now_price_master.keys():
                self.position.now_price_master[coin] = now_price_master[coin] * change
            for coin in now_price_slave.keys():
                self.position.now_price_slave[coin] = now_price_slave[coin] * change
            result[change] = self.position.cal_mr()
            detail["adjEq"] = self.position.real_adjEq
            detail["mm_master"] = self.position.mm_master
            detail["mm_slave"] = self.position.mm_slave
            detail["mm_upnl"] = self.position.mm_upnl if hasattr(self.position, "mm_upnl") else {}
            detail["fee_mm"] = self.position.fee_mm
            all_result[change] = copy.deepcopy(detail)
        return result, all_result

    
    def run_assumed_open(self):
        self.position.amount_master = {coin: 0 for coin in self.assumed_coins}
        ccy_price = self.position.get_master_price(coin = self.ccy) if self.ccy not in ["USDT", "USD", "USDC", "BUSD"] else 1
        now_price = self.position.get_now_price_master()
        self.position.price_master = now_price
        self.position.price_slave = copy.deepcopy(self.position.price_master)
        coins_number = len(self.assumed_coins)
        assumed_open: dict[float, dict[float, dict[float, float]]] = {}
        detail_open: dict[float, dict[float, dict[float, dict[str, float]]]] = {}
        for num in self.num_range:
            ret = {}
            all_ret = {}
            for mul in self.mul_range:
                mul = round(mul, 2)
                single_mv = mul / coins_number * num * ccy_price
                assumed_holding = {coin: single_mv / now_price[coin] if coin not in self.short else - single_mv / now_price[coin] for coin in self.assumed_coins}
                self.position.equity = {self.ccy: num}
                self.position.adjEq = num * ccy_price
                self.position.start_adjEq = num * ccy_price
                self.position.liability = num * ccy_price * mul
                self.position.amount_master = copy.deepcopy(assumed_holding)
                self.position.amount_slave = {coin : - amount for coin, amount in assumed_holding.items()}
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
    
    def run_account_mr(self, client: str, username: str) -> dict[float, float]:
        self.position.client = client
        self.position.username = username
        self.position.cal_mr()
        self.account_mr, self.detail_mr = self.run_price_influence()
        return self.account_mr