from cr_monitor.position.positionOkex import PositionOkex
from cr_assis.account.accountOkex import AccountOkex
import pandas as pd
import numpy as np
import copy, datetime

class MrOkex(object):
    
    def __init__(self) -> None:
        self.position = PositionOkex()
        self.price_range = np.arange(0.3, 3, 0.1)
        self.btc_num = np.arange(10, 100, 10)
    
    def run_price_influence(self, now_price: pd.DataFrame, equity: dict[str, float]) -> dict[float, float]:
        price_influence = {}
        for change in self.price_range:
            change = round(change, 2)
            self.position.now_price = now_price * change
            self.position.equity = copy.deepcopy(equity)
            price_influence[change] = self.position.cal_mr()
        return price_influence
    
    def change_position(self, account: AccountOkex, contract: str, coin: str, mv: float, now_price = pd.DataFrame()):
        account.get_equity() if not hasattr(account, "adjEq") else None
        coin = coin.upper()
        price = account.now_price.loc[coin, contract] if coin in account.now_price.index and contract in account.now_price.columns else account.get_coin_price(coin)
        if contract.split("-")[0] != "usd":
            amount = account.adjEq * mv / self.position.get_coin_price(coin) if len(now_price) == 0 else account.adjEq * mv / now_price.loc[coin, "usdt"]
            if coin in account.now_position.index and contract in account.now_position.columns and not np.isnan(account.now_position.loc[coin, contract]):
                if account.now_position.loc[coin, contract] * amount > 0:
                    account.open_price.loc[coin, contract] = abs(account.open_price.loc[coin, contract] * account.now_position.loc[coin, contract] + amount * price) / abs(account.now_position.loc[coin, contract]+amount)
                elif abs(account.now_position.loc[coin, contract]) < abs(amount):
                    account.open_price.loc[coin, contract] = price
                account.now_position.loc[coin, contract] += amount
            else:
                account.open_price.loc[coin, contract] = price
                account.now_position.loc[coin, contract] = amount
        else:
            amount = account.adjEq * mv / self.position.data_okex.get_contractsize_cswap(coin)
            if coin in account.usd_position.index and contract in account.usd_position.columns and not np.isnan(account.now_position.loc[coin, contract]):
                if account.now_position.loc[coin, contract] * amount > 0:
                    account.open_price.loc[coin, contract] = abs(account.open_price.loc[coin, contract] * account.usd_position.loc[coin, contract] + amount * price) / abs(account.usd_position.loc[coin, contract]+amount)
                elif abs(account.now_position.loc[coin, contract]) < abs(amount):
                    account.open_price.loc[coin, contract] = price
                account.usd_position.loc[coin, contract] += amount
            else:
                account.open_price.loc[coin, contract] = price
                account.usd_position.loc[coin, contract] = amount
    
    def add_account_position(self, account: AccountOkex, combo: str, add_coin: dict[str, float], now_price = pd.DataFrame()):
        master, slave = combo.split("-")
        master = master.replace("okx_", "").replace("okex_", "").replace("spot", "usdt").replace("_", "-")
        slave = slave.replace("okx_", "").replace("okex_", "").replace("spot", "usdt").replace("_", "-")
        account.get_equity() if not hasattr(account, "adjEq") else None
        for coin, mv in add_coin.items():
            coin = coin.upper()
            now_mv = account.now_position.loc[coin, master] * account.now_price.loc[coin, master] / account.adjEq if coin in account.now_position.index else 0
            self.change_position(account, master, coin, mv-now_mv, now_price) if coin != "BETH" or master == "usdt" else self.change_position(account, master, "ETH", now_mv-mv, now_price)
            self.change_position(account, slave, coin, now_mv-mv, now_price) if coin != "BETH" or slave == "usdt" else self.change_position(account, slave, "ETH", now_mv-mv, now_price)
        account.now_position.fillna(0, inplace= True)
    
    def run_account_mr(self, account: AccountOkex, add: dict[str, dict[str, float]] = {}) -> pd.DataFrame:
        account.get_now_position()
        account.get_open_price()
        account.get_now_price()
        # account.get_cashBal(account.ccy)
        for combo in add.keys():
            self.add_account_position(account, combo, add[combo])
        self.position.now_position = account.now_position[self.position.contracts].copy()
        self.position.now_position = pd.concat([self.position.now_position, pd.DataFrame(index = list(set(account.usd_position.index) - set(account.now_position.index)), columns = self.position.contracts).fillna(0)])
        self.position.now_position.loc[account.usd_position.index, account.usd_position.columns] = account.usd_position
        self.position.open_price = account.open_price
        ret = self.run_price_influence(now_price=account.now_price.copy(), equity={})
        self.account_mr = copy.deepcopy(ret)
        return ret
    
    def assumed_open(self, add: dict[str, dict[str, float]], now_price = pd.DataFrame()) -> dict[str, dict[str, float]]:
        account = AccountOkex(deploy_id="1_1")
        self.assumed_result = {}
        for num in self.btc_num:
            num = round(num, 3)
            account.adjEq = num * self.position.get_coin_price("btc") if len(now_price) == 0 else num * now_price.loc["BTC", "usdt"]
            account.now_position = pd.DataFrame(columns = self.position.contracts)
            account.usd_position = pd.DataFrame()
            for combo in add.keys():
                self.add_account_position(account, combo, add[combo], now_price)
            self.position.now_position = account.now_position[self.position.contracts].copy()
            self.position.now_position = pd.concat([self.position.now_position, pd.DataFrame(index = list(set(account.usd_position.index) - set(account.now_position.index)), columns = self.position.contracts).fillna(0)])
            self.position.now_position.loc[account.usd_position.index, account.usd_position.columns] = account.usd_position
            if len(now_price) == 0:
                account.get_now_price()
            else:
                account.now_price = now_price
            self.position.open_price = account.now_price.copy()
            ret = self.run_price_influence(now_price=account.now_price.copy(), equity={"BTC": num})
            self.assumed_result[num] = copy.deepcopy(ret)
        return self.assumed_result
    
    def run_history_price(self, history_price: dict[str, pd.DataFrame], equity: dict[str, float]) -> dict[datetime.datetime, float]:
        price_influence = {}
        ts = set(history_price["BTC"].index)
        for data in history_price.values():
            ts = ts & set(data.index)
        for t in ts:
            self.position.now_price = pd.DataFrame(columns = self.position.contracts)
            for coin in self.position.open_price.index:
                for col in history_price[coin].columns:
                    self.position.now_price.loc[coin, col] = history_price[coin].loc[t, col]
            self.position.now_price = self.position.now_price.fillna(method='ffill', axis = 1).fillna(method = 'backfill', axis = 1)
            self.position.equity = copy.deepcopy(equity)
            price_influence[t] = self.position.cal_mr()
        return price_influence
        
    def assumed_history_open(self, add: dict[str, dict[str, float]], open_price: pd.DataFrame, history_price: dict[str, pd.DataFrame]) -> dict[str, dict[str, float]]:
        account = AccountOkex(deploy_id="1_1")
        self.assumed_history_result = {}
        for num in self.btc_num:
            num = round(num, 0)
            account.adjEq = num * open_price.loc["BTC", "USDT"]
            account.now_position = pd.DataFrame(columns = self.position.contracts)
            account.usd_position = pd.DataFrame()
            for combo in add.keys():
                self.add_account_position(account, combo, add[combo], open_price)
            self.position.now_position = account.now_position[self.position.contracts].copy()
            self.position.now_position.loc[account.usd_position.index, account.usd_position.columns] = account.usd_position
            self.position.open_price = open_price.copy()
            self.assumed_history_result[num] = copy.deepcopy(self.run_history_price(history_price, equity={"BTC": num}))
        return self.assumed_history_result