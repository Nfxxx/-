from cr_monitor.mr.mrOkex import MrOkex
from cr_monitor.position.positionOkex import PositionOkex
from cr_monitor.daily.daily_monitor import set_color, set_funding_color, set_mv_color
from cr_assis.account.initAccounts import InitAccounts
from cr_assis.account.accountOkex import AccountOkex
from cr_assis.connect.connectData import ConnectData
import pandas as pd
import numpy as np
from cr_assis.pnl.ssfoPnl import SsfoPnl
from cr_assis.eva import eva
import datetime

class DailyOkex(object):
    
    def __init__(self, ignore_test = True):
        self.ignore_test = ignore_test
        self.database = ConnectData()
        self.mr_okex = MrOkex()
        self.position_okex = PositionOkex()
        self.accounts: dict[str, AccountOkex]
        self.all_position: pd.DataFrame = pd.DataFrame()
        self.position_change: pd.DataFrame = pd.DataFrame()
        self.end_date = (datetime.datetime.utcnow() + datetime.timedelta(hours = 8)).date()
        self.start_date = self.end_date + datetime.timedelta(days = -31)
        self.mv_color = {
            "okex_usd_swap-okex_usdc_swap" : "red",
            "okex_usd_swap-okex_usdt_swap" : "orange",
            "okex_spot-okex_usdt_swap": "green",
            "okex_usdt_swap-okex_spot": "green",
            "okex_usd_swap-okex_spot": "royalblue",
            "okex_spot-okex_usdt_future": "violet",
            "okex_usd_future-okex_spot": "grey",
            "okex_usdc_swap-okex_usdt_swap" : "pink",
            "okex_usdc_swap-okex_spot" : "purple",
            "okex_spot-okex_usdc_swap" : "purple",
        }
        self.init_accounts()
        self.get_pnl_daily = SsfoPnl(accounts = list(self.accounts.values()))
    
    def init_accounts(self) -> None:
        """init okex accounts"""
        self.init = InitAccounts(ignore_test= self.ignore_test)
        self.accounts = self.init.init_accounts_okex()
    
    def get_all_position(self, is_color = True, is_funding = True):
        self.color = pd.DataFrame()
        self.all_position: pd.DataFrame = pd.DataFrame()
        self.position_funding : pd.DataFrame = pd.DataFrame()
        for name, account in self.accounts.items():
            position = account.get_account_position()
            for i in position.index:
                coin = position.loc[i, "coin"].upper()
                self.all_position.loc[coin, name] = position.loc[i, "MV%"] / 100
                self.color.loc[coin, name] = "background-color: " + self.mv_color[position.loc[i, "combo"]] if position.loc[i, "combo"] in self.mv_color.keys() else "background-color: " + "black"
                if coin not in self.position_funding.index and type(position.loc[i, "combo"]) == str and is_funding:
                    kind1 = position.loc[i, "combo"].split("-")[0].split("_")[1]
                    kind2 = position.loc[i, "combo"].split("-")[1].split("_")[1]
                    ret, _ = self.run_short_chance(kind1 = kind2, kind2 = kind1, input_coins = [coin]) if kind1 == "spot" and kind2 != "spot" else self.run_short_chance(kind1 = kind1, kind2 = kind2, input_coins = [coin])
                    if (position.loc[i, "side"] == "long" and "spot" != kind1) or (position.loc[i, "side"] == "short" and "spot" == kind1):
                        ret = - ret
                        ret["vol_24h"] = abs(ret["vol_24h"])
                    self.position_funding = pd.concat([self.position_funding, ret])
        if is_funding:
            for coin in self.all_position.index:
                for col in self.position_funding.columns:
                    self.color.loc[coin, col] = "background-color: red" if coin in self.position_funding.index and self.position_funding.loc[coin, col] <0 else "background-color: black"
        self.all_position = self.all_position.fillna(0).sort_index(axis=0)
        self.all_position = pd.merge(self.all_position, self.position_funding, left_index = True, right_index = True, how = "outer") if is_funding else self.all_position
        self.color.fillna("background-color: black", inplace = True)
        format_dict = {col: '{0:.4%}' for col in self.all_position.columns if col != "vol_24h"}
        format_dict["vol_24h"] = lambda x: format(int(x), ",") if not np.isnan(x) else "nan"
        ret = self.all_position.copy() if not is_color else self.all_position.style.apply(set_mv_color, axis=None, color = self.color).format(format_dict)
        return ret
    
    def get_position_change(self, start: str, end: str, is_color = True):
        for name, account in self.accounts.items():
            old_position = account.get_account_position(the_time=start).set_index("coin")
            new_position = account.get_account_position(the_time=end).set_index("coin")
            for coin in new_position.index:
                change = new_position.loc[coin, "MV%"] - old_position.loc[coin, "MV%"] if coin in old_position.index and old_position.loc[coin, "combo"] == new_position.loc[coin, "combo"] else \
                    new_position.loc[coin, "MV%"]
                self.position_change.loc[coin.upper(), name] = change / 100
        self.position_change = self.position_change.fillna(0).sort_index(axis=0)
        format_dict = {col: '{0:.4%}' for col in self.position_change.columns}
        ret = self.position_change.copy() if not is_color else self.position_change.style.background_gradient(cmap='Blues', subset = list(self.position_change.columns)).format(format_dict)
        return ret
    
    def get_account_mr(self, is_color = True, add = {}):
        self.account_mr: dict[str, dict[str, float]] = {}
        for name, account in self.accounts.items():
            self.mr_okex = MrOkex()
            self.account_mr[name] = self.mr_okex.run_account_mr(account = account, add = add)
        ret = pd.DataFrame.from_dict(self.account_mr)
        ret = ret.style.applymap(set_color) if is_color else ret
        return ret
    
    def get_now_situation(self) -> pd.DataFrame:
        """get account situation now, like MV%, capital, ccy, mr

        Returns:
            pd.DataFrame: columns = ["account", "capital", "ccy", "MV", "MV%", "mr"]
        """
        now_situation = pd.DataFrame(columns = ["account", "capital", "ccy", "MV", "MV%", "mr"], index = range(len(self.accounts)))
        i = 0
        tickers = AccountOkex("1_1").get_tickers(instType="SPOT")
        for name, account in self.accounts.items():
            account.tickers["SPOT"] = tickers
            account.get_account_position()
            mv, mv_precent = account.position["MV"].sum(), account.position["MV%"].sum() / 100
            account.get_mgnRatio()
            capital = account.get_mean_equity()
            ccy = account.ccy
            mr = account.mr["okex"]
            now_situation.loc[i] = [name, capital, ccy, mv, mv_precent, mr]
            i+=1
        self.now_situation = now_situation.copy()
        format_dict = {'capital': lambda x: format(round(x, 4), ","),  
                        'MV': lambda x: format(round(x, 2), ","), 
                        'MV%': '{0:.2%}', 
                        'mr': lambda x: format(round(x, 2), ",")}
        now_situation = now_situation.style.applymap(set_funding_color).format(format_dict)
        return now_situation
    
    def run_daily(self, is_fpnl = False) -> pd.DataFrame:
        rpnl = self.get_pnl_daily.get_rpnl()
        fpnl, ipnl, tpnl = self.get_pnl_daily.get_fpnl() if is_fpnl else ({}, {}, {})
        self.get_now_situation()
        account_overall = self.now_situation.copy()
        for i in account_overall.index:
            parameter_name = account_overall.loc[i, "account"]
            for day in [1, 3, 7]:
                account_overall.loc[i, f"{day}d_pnl%"] = rpnl[parameter_name][day]
                if is_fpnl:
                    account_overall.loc[i, f"{day}d_tpnl%"] = tpnl[parameter_name][day]
                    account_overall.loc[i, f"{day}d_fpnl%"] = fpnl[parameter_name][day]
                    account_overall.loc[i, f"{day}d_ipnl%"] = ipnl[parameter_name][day]
        self.account_overall = account_overall.copy()
        format_dict = {'capital': lambda x: format(round(x, 4), ","), 
                        'MV': lambda x: format(int(x), ","), 
                        'MV%': '{0:.2%}', 
                        'mr': lambda x: format(round(x, 2), ",")}
        for col in account_overall.columns:
            if "pnl%" in col:
                format_dict[col] = '{0:.4%}'
        account_overall = account_overall.style.applymap(set_funding_color).format(format_dict)
        return account_overall
    
    def handle_funding_summary(self, funding_summary: pd.DataFrame, kind1: str, kind2: str):
        funding_summary = funding_summary.drop(["last_dt", "1t"], axis = 1).dropna(subset = ["1d"]).rename(columns = {"volume_U_24h": "vol_24h"})
        for col in ["1d", "3d", "7d", "15d", "30d"]:
            num = int(col.split("d")[0]) * 3
            funding_summary[col + "_avg"] = funding_summary[col] / num
        rate = pd.DataFrame(columns = ["next", "current"])
        for coin in funding_summary.index:
            data0 = eva.get_last_influx_funding(exchange_name="okex", pair_name=f"""{coin.lower().replace("beth", "eth")}-{kind1}-swap""") if kind1 != "spot" else pd.DataFrame.from_dict({"next_fee": {0: 0}, "rate": {0: 0}})
            data1 = eva.get_last_influx_funding(exchange_name="okex", pair_name=f"""{coin.lower().replace("beth", "eth")}-{kind2}-swap""") if kind2 != "spot" else pd.DataFrame.from_dict({"next_fee": {0: 0}, "rate": {0: 0}})
            rate.loc[coin] = [data0["next_fee"].values[-1] - data1["next_fee"].values[-1], data0["rate"].values[-1] - data1["rate"].values[-1]]
            if coin.upper() == "BETH":
                rate.loc[coin] += eva.get_eth2_staking() / 365 / 3
        funding_summary = pd.concat([rate, funding_summary], axis = 1)
        return funding_summary
    
    def run_long_chance(self, kind1: str, kind2: str, start_date= datetime.date(2021,1,1), input_coins = ["BTC", "ETH"]):
        funding_summary, funding, _ = eva.run_funding("okex", kind1, "okex", kind2, start_date, self.end_date, play = True, input_coins = input_coins)
        funding = funding.T
        funding_summary = self.handle_funding_summary(funding_summary, kind1, kind2)
        return funding_summary, funding
    
    def run_short_chance(self, kind1: str, kind2: str, input_coins = []):
        funding_summary, funding, _ = eva.run_funding("okex", kind1, "okex", kind2, self.start_date, self.end_date, play = False, input_coins = input_coins)
        funding = funding.T
        funding_summary = self.handle_funding_summary(funding_summary, kind1, kind2)
        return funding_summary, funding
    
    def format_funding_summary(self, funding_summary):
        format_dict = {}
        result = funding_summary.copy()
        for col in result.columns:
            if col != "vol_24h":
                format_dict[col] = '{0:.3%}'
            else:
                format_dict[col] = lambda x: format(int(x), ",") if not np.isnan(x) else "nan"
        return result.style.applymap(set_funding_color).format(format_dict)
    
    def get_dtc_funding(self):
        self.dtc_summary, self.dtc_funding = self.run_long_chance(kind1 = "usdc", kind2 = "usd", start_date= datetime.date(2022,10,1))
        return self.format_funding_summary(self.dtc_summary)
    
    def get_ssfc_funding(self):
        self.ssfc_summary, self.ssfc_funding = self.run_long_chance(kind1 = "usdc", kind2 = "spot", start_date= datetime.date(2022,10,1))
        return self.format_funding_summary(self.ssfc_summary)
    
    def get_bu_funding(self):
        self.bu_summary, self.bu_funding = self.run_long_chance(kind1 = "usdc", kind2 = "usdt", start_date= datetime.date(2022,10,1))
        return self.format_funding_summary(self.bu_summary)
    
    def get_dt_funding(self, input_coins = ["BTC", "ETH", "XRP", "ADA", "ETC"]):
        self.run_long_chance(kind1 = "usdt", kind2 = "usd", input_coins = input_coins)
        self.dt_summary, self.dt_funding = self.run_short_chance(kind1 = "usdt", kind2 = "usd")
        self.dt_summary = pd.concat([self.dt_summary.loc[input_coins], self.dt_summary.sort_values(by = "15d", ascending = False)]).drop_duplicates(keep = "first")
        return self.format_funding_summary(self.dt_summary)
    
    def get_ssf_funding(self, input_coins = ["FIL", "BTC", "DOGE", "LTC", "ETH"], save_path = "/home/ssh/jupyter/data/daily_report/SSFO.xlsx"):
        self.run_long_chance(kind1 = "usdt", kind2 = "spot", input_coins = input_coins) if input_coins != [] else None
        self.ssf_summary, self.ssf_funding = self.run_short_chance(kind1 = "usdt", kind2 = "spot")
        self.ssf_summary.to_excel(save_path, sheet_name = "ssf_summary")
    
    def get_ssfd_funding(self, input_coins = ["FIL", "BTC", "DOGE", "LTC", "ETH"], save_path = "/home/ssh/jupyter/data/daily_report/SSF_USD.xlsx"):
        self.run_long_chance(kind1 = "usd", kind2 = "spot", input_coins = input_coins) if input_coins != [] else None
        self.ssfd_summary, self.ssfd_funding = self.run_short_chance(kind1 = "usd", kind2 = "spot")
        self.ssfd_summary.to_excel(save_path, sheet_name = "ssfd_summary")