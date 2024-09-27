import os, datetime, copy, sys
import pandas as pd
import numpy as np
import research.utils.pnlDaily as pnl_daily
from pymongo import MongoClient
from cr_assis.connect.connectData import ConnectData
from cr_assis.account.accountBase import AccountBase
from research.eva import eva
from cr_monitor.mr.Mr_DTO import MrDTO

class DailyMonitorDTO(object):
    def __init__(self, ignore_test = True):
        self.ignore_test = ignore_test
        self.strategy_name = "dt_okex_cswap_okex_uswap"
        self.init_accounts()
        self.database = ConnectData()
        self.get_pnl_daily = pnl_daily
    
    def get_now_parameter(self, deploy_id: str) -> pd.DataFrame:
        """获得mongo里面相应账户的信息"""
        client = deploy_id.split("_")[0]
        mongo_clt = MongoClient(os.environ["MONGO_URI"])
        a = mongo_clt["Strategy_deploy"][client].find({"_id": deploy_id})
        data = pd.DataFrame(a)
        data = data[data["_id"] == deploy_id].copy()
        data.index = range(len(data))
        return data
    
    def get_all_deploys(self) -> list:
        #获得所有启动的账户deploy_id
        mongo_clt = MongoClient(os.environ["MONGO_URI"])
        collections = mongo_clt["Strategy_orch"].list_collection_names()
        deploy_ids = []
        for key in collections:
            a = mongo_clt["Strategy_orch"][key].find()
            data = pd.DataFrame(a)
            data = data[(data["orch"]) & (data["version"] != "0") & (data["version"] != None) & (data["version"] != "0")].copy()
            deploy_ids += list(data["_id"].values)
        deploy_ids.sort()
        return deploy_ids
    
    def get_strategy_info(self, strategy: str):
        """解析deployd_id的信息"""
        words = strategy.split("_")
        master = (words[1] + "_" + words[2]).replace("okexv5", "okx").replace("okex", "okx")
        master = master.replace("uswap", "usdt_swap")
        master = master.replace("cswap", "usd_swap")
        master = master.replace("ufuture", "usdt_future")
        master = master.replace("cfuture", "usd_future")
        slave = (words[3] + "_" + words[4]).replace("okexv5", "okx").replace("okex", "okx")
        slave = slave.replace("uswap", "usdt_swap")
        slave = slave.replace("cswap", "usd_swap")
        slave = slave.replace("ufuture", "usdt_future")
        slave = slave.replace("cfuture", "usd_future")
        ccy = words[-1].upper()
        if ccy == "U":
            ccy = "USDT"
        elif ccy == "C":
            ccy = "BTC"
        else:
            pass
        return master, slave, ccy
    
    def get_bbu_info(self, strategy: str):
        """解析bbu线的deploy_id信息"""
        words = strategy.split("_")
        exchange = words[1].replace("okexv5", "okx").replace("okex", "okx")
        if exchange == "binance":
            master = "binance_busd_swap"
        else:
            master = f"{exchange}_usdc_swap"
        slave = f"{exchange}_usdt_swap"
        ccy = strategy.split("_")[-1].upper()
        if ccy in ["U", "BUSD"]:
            ccy = "USDT"
        else:
            pass
        return master, slave, ccy
    
    def init_accounts(self, is_usdc = False) -> None:
        """初始化所有指定策略线账户"""
        deploy_ids = self.get_all_deploys()
        accounts = {}
        for deploy_id in deploy_ids:
            parameter_name, strategy = deploy_id.split("@")
            client, username = parameter_name.split("_")
            judgement1 = self.strategy_name in strategy
            if self.ignore_test:
                judgement2 = client not in ["test", "lxy"]
            else:
                judgement2 = True
            if judgement1 and judgement2:
                #只监控指定策略线账户
                accounts[parameter_name] = AccountBase(deploy_id = deploy_id, is_usdc= is_usdc)
            else:
                pass
        self.accounts = accounts.copy()
    
    def get_account_upnl(self) -> dict:
        """获得各个账户的upnl，用现在的现货价格减去开仓价格来计算"""
        position = {}
        for account in self.accounts.values():
            if not hasattr(account, "now_position"):
                now_position = account.get_now_position()
            else:
                now_position = account.now_position
            if len(now_position) >0:
                for coin in now_position.index:
                    if account.contract_slave != "-usd-swap":
                        number = now_position.loc[coin, "slave_number"]
                    else:
                        number = now_position.loc[coin, "master_number"]
                    price = account.get_coin_price(coin = coin)
                    upnl = abs(price - now_position.loc[coin, "slave_open_price"]) * number
                    now_position.loc[coin, "upnl"] = upnl
            else:
                now_position.loc[coin, "upnl"] = 0
            position[account.parameter_name] = now_position.copy()
        return position
    
    def get_last_equity(self, account) -> float:
        ccy = account.principal_currency.lower()
        a = f"""
        select mean({ccy}) as equity, balance_id from balance_v2 where username = '{account.username}' and client = '{account.client}' and time >= now() - 1d
        """
        data = self.database._send_influx_query(a, database = "account_data")
        if len(data) == 0:
            a = f"""
            select mean({ccy}) as equity, balance_id from balance_v2 where username = '{account.username}' and client = '{account.client}' and time >= now() - 1d
            """
            data = self.database._send_influx_query(a, database = "account_data")
        if len(data) == 0:
            data = pd.DataFrame(columns = ["equity"])
            data.loc[0, "equity"] = np.nan
        equity = data.loc[0, "equity"]
        return equity

    def get_7d_equity(self, account) -> float:
        ccy = account.principal_currency.lower()
        a = f"""
        select mean({ccy}) as equity from balance_v2 where balance_id = '{account.balance_id}' and time >= now() - 7d - 30m and time <= now() - 7d
        """
        data = self.database._send_influx_query(a, database = "account_data")
        if len(data) == 0:
            a = f"""
            select last({ccy}) as equity from balance_v2 where balance_id = '{account.balance_id}' and time >= now() - 8d and time <= now() - 6d
            """
            data = self.database._send_influx_query(a, database = "account_data")
        if len(data) == 0:
            data = pd.DataFrame(columns = ["equity"])
            data.loc[0, "equity"] = np.nan
        equity = data.loc[0, "equity"]
        return equity

    def get_week_profit(self, account: AccountBase) -> float:
        equity = {}
        equity["now"] = account.get_mean_equity()
        equity["7d"] = account.get_mean_equity(the_time="now()-7d")
        profit = equity["now"] / equity["7d"] - 1
        return profit
    
    def get_now_situation(self) -> pd.DataFrame:
        """get account situation now, like MV%, capital, ccy, mr

        Returns:
            pd.DataFrame: columns = ["account", "capital", "ccy", "MV", "MV%", "mr", "week_profit"]
        """
        accounts = list(self.accounts.values())
        now_situation = pd.DataFrame(columns = ["account", "capital", "ccy", "MV", "MV%", "mr", "week_profit"], index = range(len(accounts)))
        for i in now_situation.index:
            account = accounts[i]
            account.get_equity()
            account.get_account_position()
            account.get_mgnRatio()
            capital_price = 1 if "USD" in account.principal_currency else account.get_coin_price(coin = account.principal_currency.lower())
            capital = account.adjEq / capital_price
            ccy = account.principal_currency
            if hasattr(account, "position"):
                mv = sum(account.position["MV"].values)
                mv_precent = sum(account.position["MV%"].values)
            else:
                mv = np.nan
                mv_precent = np.nan
            mr = account.mr["okex"]
            profit = self.get_week_profit(account)
            now_situation.loc[i] = [account.parameter_name, capital, ccy, mv, mv_precent, mr, profit]
        self.now_situation = now_situation.copy()
        format_dict = { 'MV': '{0:.2f}', 
                        'MV%': '{0:.2f}', 
                        'mr': lambda x: format(round(x, 2), ","),
                        'week_profit': '{0:.4%}'}
        now_situation = now_situation.style.applymap(set_funding_color).format(format_dict)#.background_gradient(cmap='Blues', subset = ["MV%", "mr", 'week_profit'])
        return now_situation
            
    def run_daily(self) -> pd.DataFrame:
        result, account_overall = self.get_pnl_daily.run_daily_pnl(accounts = list(self.accounts.values()), save_excel = False)
        for i in account_overall.index:
            parameter_name = account_overall.loc[i, "account"]
            #adjEq
            adjEq = self.accounts[parameter_name].get_mean_equity()
            account_overall.loc[i, "capital"] = adjEq
            #total MV%
            self.accounts[parameter_name].get_account_position()
            if hasattr(self.accounts[parameter_name], "position"):
                account_overall.loc[i, "MV%"] = sum(self.accounts[parameter_name].position["MV%"].values)
            else:
                account_overall.loc[i, "MV%"] = 0
            #mr
            self.accounts[parameter_name].get_mgnRatio()
            account_overall.loc[i, "mr"] = self.accounts[parameter_name].mr["okex"]
            # #upnl
            # upnl = sum(position[parameter_name]["upnl"].values)
            # account_overall.loc[i, "upnl"] = upnl
            # week_profit
            profit = self.get_week_profit(self.accounts[parameter_name])
            account_overall.loc[i, "week_profit"] = profit
            
        self.account_overall = account_overall.copy()
        format_dict = {'capital': lambda x: format(round(x, 4), ","), 
                        'daily_pnl': '{0:.4f}', 
                        'daily_pnl%': '{0:.4%}', 
                        #'规模对应日期':lambda x: "{}".format(x.strftime('%Y%m%d')),
                        'combo_avg': '{0:.4%}', 
                        'MV%': '{0:.2f}', 
                        'mr': lambda x: format(round(x, 2), ","),
                        'week_profit': '{0:.4%}'
                        }
        account_overall = account_overall.style.applymap(set_funding_color).format(format_dict)#.background_gradient(cmap='Blues', subset = ["daily_pnl", "daily_pnl%", "MV%", "mr", 'week_profit'])
        return account_overall
    
    def get_change(self):
        result, funding = eva.observe_dt_trend()
        result.dropna(subset = "vol_24h", inplace = True)
        funding.dropna(how = "all", inplace = True)
        self.funding_summary = copy.deepcopy(result)
        self.funding = copy.deepcopy(funding)
        format_dict = {}
        for col in result.columns:
            if col != "vol_24h":
                result[col] = result[col].apply(lambda x: float(x.split("%")[0])/100)
                format_dict[col] = '{0:.3%}'
            else:
                result[col] = result[col].apply(lambda x: float(x.replace(",", "")) if type(x) == str else np.nan)
                format_dict[col] = lambda x: format(round(x, 0), ",")
        funding_summary = result.style.applymap(set_funding_color).format(format_dict)#.background_gradient(cmap='Blues')
        return funding_summary
    
    def get_coin_parameter(self, coin: str, suffix = "-usd-swap") -> pd.DataFrame:
        data = pd.DataFrame(columns = ["open", "close_maker","position", "close_taker",
                            "open2", "close_maker2", "position2", "close_taker2",
                            "fragment", "fragment_min", "side","funding_fee_loss_stop_open", "funding_fee_profit_stop_close", "timestamp"])
        contract = f"{coin}{suffix}"
        for name, account in self.accounts.items():
            origin_data = account.get_now_parameter()
            account.get_now_position()
            if coin in account.now_position.index.values:
                side = account.now_position.loc[coin, "side"]
            elif contract in origin_data.loc[0, "spreads"]:
                if "long" in origin_data.loc[0, "spreads"][contract]:
                    side = "long"
                else:
                    side = "short"
            if contract in origin_data.loc[0, "spreads"]:
                parameter = origin_data.loc[0, "spreads"][contract]
                timestamp = origin_data.loc[0, "_comments"]["timestamp"]
                for col in ["open", "close_maker","position", "close_taker"]:
                    data.loc[name, col] = parameter[side][0][col]
                for col in ["open2", "close_maker2","position2", "close_taker2"]:
                    data.loc[name, col] = parameter[side][1][col.split("2")[0]]
                for col in ["fragment", "fragment_min", "funding_fee_loss_stop_open", "funding_fee_profit_stop_close"]:
                    data.loc[name, col] = parameter["ctrl"][col]
                data.loc[name, "side"] = side
                data.loc[name, "timestamp"] = timestamp
        return data
    
    def get_btc_parameter(self):
        data = self.get_coin_parameter(coin = "btc")
        self.btc_parameter = data.copy()
    
    def get_eth_parameter(self):
        data = self.get_coin_parameter(coin = "eth")
        self.eth_parameter = data.copy()
    
    def run_mr(self):
        #推算每个账户的mr情况
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
                mr_dto = MrDTO(amount_u = now_position.loc["btc", "slave_number"] * 100,
                            amount_c = now_position.loc["btc", "master_number"],
                            amount_fund = account.adjEq / now_price,
                            price_u = now_position.loc["btc", "slave_open_price"], 
                            price_c = now_position.loc["btc", "master_open_price"],
                            now_price = now_price)
                mr_dto.run_mmr(play = False)
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
def set_color(val):
    #set mr color
    if val <=3:
        color = 'red'
    elif val <=5:
        color = 'orange'
    else:
        color = 'green'
    return 'background-color: %s' % color

def set_funding_color(val):
    ret = None
    if type(val) == str or np.isnan(val):
        return 
    elif val >= 0:
        return None
    else:
        return 'background-color: red'
    
def set_mv_color(df: pd.DataFrame, color: pd.DataFrame) -> str:
    return color