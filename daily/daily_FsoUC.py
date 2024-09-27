from cr_monitor.daily.daily_monitor import set_color
from cr_monitor.daily.daily_DTFmonitor import DailyMonitorDTF
from cr_monitor.mr.MrFso_UC import FsoUC
import copy, sys, os
from cr_assis.pnl.fsoPnl import FsoPnl
import pandas as pd
import numpy as np

class DailyFsoUC(DailyMonitorDTF):
    def __init__(self, delivery="230331", ignore_test=True):
        self.delivery = delivery
        self.ignore_test = ignore_test
        self.strategy_name = "dt_okex_uswap_okex_cfuture"
        self.init_accounts()
        self.get_pnl_daily = FsoPnl(accounts = list(self.accounts.values()))
    
    def get_btc_parameter(self):
        data = self.get_coin_parameter(coin = "btc", suffix=f"-usdt-swap")
        self.btc_parameter = data.copy()
    
    def get_eth_parameter(self):
        data = self.get_coin_parameter(coin = "eth", suffix=f"-usdt-swap")
        self.eth_parameter = data.copy()
    
    def run_mr(self):
        """推算每个账户的mr情况"""
        self.mgnRatio = {}
        self.picture_value = pd.DataFrame()
        self.picture_spread = pd.DataFrame()
        now_price = list(self.accounts.values())[0].get_coin_price(coin = "btc")
        for name, account in self.accounts.items():
            if not hasattr(account, "now_position"):
                now_position = account.get_now_position()
            else:
                now_position = account.now_position
            if "btc" in now_position.index:
                account.get_equity()
                #初始化账户
                mr_dto = FsoUC(amount_c = now_position.loc["btc", "slave_number"],
                                amount_u = round(now_position.loc["btc", "master_number"] * 100, 0),
                                amount_fund = account.adjEq / now_price,
                                price_u = now_position.loc["btc", "master_open_price"], 
                                price_c = now_position.loc["btc", "slave_open_price"],
                                now_price = now_price, 
                                suffix = self.delivery)
                mr_dto.run_mmr(play = False)
                #保留数据
                self.mgnRatio[name] = copy.deepcopy(mr_dto)
                self.picture_value = pd.concat([mr_dto.value_influence, self.picture_value], axis = 1, join = 'outer')
                self.picture_spread = pd.concat([mr_dto.spread_influence, self.picture_spread], axis = 1, join = 'outer')
                self.picture_value.rename({"mr": name}, inplace = True, axis = 1)
                self.picture_spread.rename({"mr": name}, inplace = True, axis = 1)
        value = copy.deepcopy(self.picture_value)
        spread = copy.deepcopy(self.picture_spread)
        value = value.style.applymap(set_color)
        spread = spread.style.applymap(set_color)
        return value, spread