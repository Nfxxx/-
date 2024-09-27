from cr_monitor.daily.daily_monitor import set_color, set_funding_color
from cr_assis.connect.connectData import ConnectData
import copy, sys, os, datetime
from cr_assis.pnl.ssfoPnl import SsfoPnl
from cr_monitor.position.Position_SSFO import PositionSSFO
from pymongo import MongoClient
import pandas as pd
import numpy as np
from research.eva import eva
from cr_assis.account.accountBase import AccountBase
from cr_assis.account.initAccounts import InitAccounts
from cr_monitor.mr.Mr_SSFO import MrSSFO

class DailySSFO(object):
    def __init__(self, ignore_test=True):
        self.ignore_test = ignore_test
        self.database = ConnectData()
        self.strategy_name = "ssf_okexv5_spot_okexv5_uswap_btc"
        self.combo = "okx_spot-okx_usdt_swap"
        self.position = PositionSSFO
        self.mr = MrSSFO
        self.accounts: dict[str, AccountBase]
        self.init_accounts()
        self.get_pnl_daily = SsfoPnl(accounts = list(self.accounts.values()))
    
    def init_accounts(self) -> None:
        """初始化所有指定策略线账户"""
        self.init = InitAccounts(combo = self.combo, ignore_test= self.ignore_test)
        self.accounts = self.init.init_accounts(is_usdc = False)
    
    def get_now_mv_percent(self, account: AccountBase) -> float:
        account.get_equity()
        mv = 0
        position = self.position(client = account.client, username = account.username) if not hasattr(account, "position_ssfo") else account.position_ssfo
        if not hasattr(position, "origin_slave"):
            position.get_origin_slave(start = "now() - 10m", end = "now()")
            position.get_slave_mv()
        for data in position.origin_slave.values():
            data.set_index("time", inplace = True)
            data.dropna(how = "any", inplace = True)
            mv += data["mv"].values[-1] if len(data) > 0 else np.nan
        mv_precent = mv / account.adjEq
        return mv, mv_precent
    
    def get_now_situation(self) -> pd.DataFrame:
        """get account situation now, like MV%, capital, ccy, mr

        Returns:
            pd.DataFrame: columns = ["account", "capital", "ccy", "MV", "MV%", "mr"]
        """
        accounts = list(self.accounts.values())
        now_situation = pd.DataFrame(columns = ["account", "capital", "ccy", "MV", "MV%", "mr"], index = range(len(accounts)))
        for i in now_situation.index:
            account = accounts[i]
            mv, mv_precent = self.get_now_mv_percent(account)
            account.get_mgnRatio()
            capital = account.get_mean_equity()
            ccy = account.principal_currency
            mr = account.mr["okex"]
            now_situation.loc[i] = [account.parameter_name, capital, ccy, mv, round(mv_precent * 100, 4), mr]
        self.now_situation = now_situation.copy()
        format_dict = {'capital': lambda x: format(round(x, 4), ","),  
                        'MV': lambda x: format(round(x, 2), ","), 
                        'MV%': '{0:.2f}', 
                        'mr': lambda x: format(round(x, 2), ",")}
        now_situation = now_situation.style.applymap(set_funding_color).format(format_dict)#.background_gradient(cmap='Blues', subset = ["MV%", "mr", 'week_profit'])
        return now_situation
    
    def get_chance(self):
        self.funding_summary, self.funding, _ = eva.run_funding("okex", "spot", "okex", "usdt", datetime.date.today() + datetime.timedelta(days = -31), datetime.date.today(), play = False)
        self.funding_summary.drop("last_dt", inplace = True, axis = 1)
        self.funding_summary.drop("1t", inplace = True, axis = 1)
        self.funding_summary.dropna(subset = ["1d"], inplace = True)
        self.funding_summary.rename(columns = {"volume_U_24h": "vol_24h"}, inplace = True)
        for col in ["1d", "3d", "7d", "15d", "30d"]:
            num = int(col.split("d")[0]) * 3
            self.funding_summary[col + "_avg"] = self.funding_summary[col] / num
        self.funding = self.funding.T
        self.get_now_situation() if not hasattr(self, "now_situation") else None
        rate = pd.DataFrame(columns = ["next", "current"])
        for coin in self.funding_summary.index:
            data = eva.get_last_influx_funding(exchange_name="okex", pair_name=f"{coin.lower()}-usdt-swap")
            data.rename(columns = {"rate": "current", "next_fee": "next"}, inplace = True)
            data.drop("dt", inplace = True, axis = 1)
            data.drop("time", inplace = True, axis = 1)
            rate.loc[coin] = data.loc[0]
        all_position = self.get_all_position().fillna(0).drop("total")
        for account in self.accounts.values():
            self.funding_summary[account.parameter_name] = 0
            for coin in all_position.index:
                self.funding_summary.loc[coin.upper(), account.parameter_name] = all_position.loc[coin, account.parameter_name] / 100
        self.funding_summary = pd.concat([rate, self.funding_summary], axis = 1)
        result = copy.deepcopy(self.funding_summary)
        format_dict = {}
        for col in result.columns:
            if col != "vol_24h":
                format_dict[col] = '{0:.3%}'
            else:
                format_dict[col] = lambda x: format(round(x, 0), ",")
        funding_summary = result.style.format(format_dict)
        return funding_summary
    
    def get_volume_rate(self) -> pd.DataFrame:
        self.get_all_position() if not hasattr(self, "mv_monitor") else None
        self.get_chance() if not hasattr(self, "funding_summary") else None
        volume_rate = pd.DataFrame(columns = ["vol_24h", "total"])
        volume_rate["vol_24h"] = self.funding_summary["vol_24h"]
        for name, mv in self.mv_monitor.items():
            for pair, data in mv.items():
                coin = pair.split("-")[0].upper()
                df = data.dropna(subset = ["mv"])
                volume_rate.loc[coin, name] = df["mv"].values[-1] if len(df) > 0 else np.nan
        names = list(set(self.mv_monitor.keys()) & set(volume_rate.columns))
        volume_rate.fillna(0, inplace= True)
        volume_rate["total"] = volume_rate[names].sum(axis = 1)
        self.volume_rate = copy.deepcopy(volume_rate)
        format_dict = {}
        for col in volume_rate.columns:
            format_dict[col] = lambda x: format(round(x, 0), ",")
        volume_rate = volume_rate.style.format(format_dict)
        return volume_rate
    
    def daily_run_chance(self, save_file = False, save_path = "") -> tuple[pd.DataFrame.style, pd.DataFrame.style]:
        funding_summary = self.get_chance()
        volume_rate = self.get_volume_rate()
        if save_file:
            writer = pd.ExcelWriter(f"{save_path}.xlsx", engine='openpyxl')
            self.funding_summary.to_excel(excel_writer=writer, sheet_name="funding_summary")
            self.volume_rate.to_excel(excel_writer=writer, sheet_name="volume_rate")
            writer.save()
            writer.close()
        return funding_summary, volume_rate
    
    def run_daily(self, is_fpnl = False) -> pd.DataFrame:
        rpnl = self.get_pnl_daily.get_rpnl()
        fpnl = self.get_pnl_daily.get_fpnl() if is_fpnl else {}
        self.get_now_situation() if not hasattr(self, "now_situation") else None
        account_overall = self.now_situation.copy()
        for i in account_overall.index:
            parameter_name = account_overall.loc[i, "account"]
            for day in [1, 3, 7]:
                account_overall.loc[i, f"{day}d_pnl%"] = rpnl[parameter_name][day]
                if is_fpnl:
                    account_overall.loc[i, f"{day}d_fpnl%"] = fpnl[parameter_name][day]
        self.account_overall = account_overall.copy()
        format_dict = {'capital': lambda x: format(round(x, 4), ","), 
                        'MV': lambda x: format(round(x, 2), ","), 
                        'MV%': '{0:.2f}', 
                        'mr': lambda x: format(round(x, 2), ",")}
        for col in account_overall.columns:
            if "pnl%" in col:
                format_dict[col] = '{0:.4%}'
        account_overall = account_overall.style.applymap(set_funding_color).format(format_dict)#.background_gradient(cmap='Blues', subset = ["MV%", "mr", 'week_profit','1d_rpnl%', '3d_rpnl%', '7d_rpnl%','1d_fpnl%', '3d_fpnl%', '7d_fpnl%'])
        return account_overall
    
    def get_mv_monitor(self, start = "now() - 1d", end = "now()") -> dict:
        mv_monitor = {}
        for name, account in self.accounts.items():
            account.position_ssfo = self.position(client = account.client, username = account.username) if not hasattr(account, "position_ssfo") else account.position_ssfo
            position = account.position_ssfo
            position.get_origin_slave(start = start, end = end)
            position.get_slave_mv()
            account.get_equity()
            for pair, data in position.origin_slave.items():
                data['mv%'] = round(data["mv"] / account.adjEq * 100, 4)
            mv_monitor[name] = position.origin_slave.copy()
        self.mv_monitor = mv_monitor
        return mv_monitor
    
    def get_all_position(self, start = "now() - 5m", end = "now()", is_color = False):
        mv_monitor = self.get_mv_monitor(start = start, end = end)
        if len(mv_monitor) == 0:
            return pd.DataFrame(index = ["total"], columns = ["nan"])
        all_position = pd.DataFrame(columns = list(mv_monitor.keys()))
        for account in all_position.columns:
            account_position = mv_monitor[account].copy()
            for pair in account_position.keys():
                coin = pair.split("-")[0]
                all_position.loc[coin, account] = account_position[pair].fillna(method='ffill')["mv%"].values[-1] if len(account_position[pair]) > 0 else np.nan
        all_position.sort_index(axis = 0, inplace = True)
        all_position.sort_index(axis = 1, inplace = True)
        all_position.loc["total"] = all_position.sum(axis = 0) if len(all_position) > 0 else 0
        self.all_position = all_position.copy()
        if is_color:
            all_position = all_position.fillna(0).style.background_gradient(cmap='Blues', subset = list(self.all_position.columns), vmax = 25, vmin = 0)
        return all_position
    
    def get_position_change(self, start: str, end: str, is_color = False) -> pd.DataFrame:
        before = self.get_all_position(start = f"{start} - 5m", end = start).fillna(0)
        after = self.get_all_position(start = f"{end} - 5m", end = end).fillna(0)
        coins = list(set(list(before.index.values)) | set(list(after.index.values)))
        coins.remove("total")
        coins.sort()
        names = set(list(before.columns.values)) | set(list(after.columns.values))
        position_change = pd.DataFrame(index = coins + ["total"], columns = list(names))
        for coin in position_change.index:
            for name in names:
                position0 = before.loc[coin, name] if coin in before.index.values and name in before.columns.values else 0
                position1 = after.loc[coin, name] if coin in after.index.values and name in after.columns.values else 0
                delta_position = position1 - position0
                position_change.loc[coin, name] = delta_position
        position_change.sort_index(axis = 1, inplace = True)
        if is_color:
            position_change = position_change.style.background_gradient(cmap='Blues', subset = list(self.all_position.columns), vmax = 25, vmin = -25)
        return position_change

    def run_mr(self, price_range = np.arange(0.3, 2, 0.1)) -> pd.DataFrame.style:
        account_mr = {}
        for account in self.accounts.values():
            account.cal_mr = self.mr(position=self.position())
            account.cal_mr.price_range = price_range
            account_mr[account.parameter_name] = account.cal_mr.run_account_mr(client = account.client, username = account.username)
        self.account_mr = account_mr
        mr_situation = pd.DataFrame(account_mr)
        self.mr_situation = mr_situation.copy()
        mr_situation = mr_situation.style.applymap(set_color)
        return mr_situation
    
    def run_assumed_situation(self, mul_range = np.arange(0.5, 2, 0.1), price_range = np.arange(0.3, 2, 0.1)):
        mr = self.mr(position=self.position())
        mr.mul_range = mul_range
        mr.price_range = price_range
        assumed_open = mr.run_assumed_open()
        assumed_situation = pd.DataFrame()
        for num in assumed_open.keys():
            change = assumed_open[num]
            for mul in change.keys():
                assumed_situation.loc[mul, num] = change[mul][1]
        self.mr = mr
        self.assumed_situation = assumed_situation
        return assumed_situation