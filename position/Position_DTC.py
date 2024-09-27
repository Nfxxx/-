from cr_monitor.position.Position_SSFO import PositionSSFO
import pandas as pd
import numpy as np
import copy

class PositionDTC(PositionSSFO):
    
    def __init__(self, client = "", username = "") -> None:
        super().__init__(client, username)
        self.master = "usd_swap"
        self.slave = "usdc_swap"
        self.fee_rate = 0.002
        self.amount_master: dict[str, float] # the amount of contract, not coin number, not dollar value
        self.contract_master: dict[str, float] = {}
        self.upnl_master: dict[str, float]
        self.upnl_slave: dict[str, float]
        self.tier_upnl: dict[str, pd.DataFrame] = {}
        self.spot_price: dict[str, float] = {}
        
    def get_master_price(self, coin: str) -> float:
        price = self.get_redis_price(coin = coin, suffix = self.master.replace("_", "-"))
        return price

    def get_slave_price(self, coin: str) -> float:
        price = self.get_redis_price(coin = coin, suffix = self.slave.replace("_", "-"))
        return price
    
    def get_spot_price(self, coin: str) -> float:
        price = self.get_redis_price(coin = coin, suffix = "usdt")
        return price
    
    def get_contractsize_slave(self, coin: str) -> float:
        coin = coin.upper()
        contractsize = self.markets[f"{coin}/USDC:USDC"]["contractSize"] if f"{coin}/USDC:USDC" in self.markets.keys() else np.nan
        self.contract_slave[coin] = contractsize
        return contractsize
    
    def get_contractsize_master(self, coin: str) -> float:
        coin = coin.upper()
        contractsize = self.markets[f"{coin}/USD:{coin}"]["contractSize"] if f"{coin}/USD:{coin}" in self.markets.keys() else np.nan
        self.contract_master[coin] = contractsize
        return contractsize
    
    def get_tier_upnl(self, coin: str) -> pd.DataFrame:
        tier = self.get_tier_spot(coin = coin)
        self.tier_upnl[coin.upper()] = tier
        return tier
    
    def get_tier_master(self, coin: str) -> pd.DataFrame:
        tier = self.get_tier_swap(coin = coin, contract = self.master.split("_")[0].upper())
        self.tier_master[coin.upper()] = tier
        return tier
    
    def get_tier_slave(self, coin: str) -> pd.DataFrame:
        tier = self.get_tier_swap(coin = coin, contract = self.slave.split("_")[0].upper())
        self.tier_slave[coin.upper()] = tier
        return tier
    
    def get_origin_slave(self, start: str, end: str) -> dict:
        ret = super().get_origin_slave(start, end)
        pairs = list(ret.keys())
        name = self.slave.split("_")[0]
        for pair in pairs:
            if name != pair.split("-")[1]:
                del ret[pair]
        self.origin_slave = ret
        return ret
    
    def get_equity(self, ccy = "BTC") -> dict[str, float]:
        amount = self.get_cashBalance(ccy = ccy)
        self.equity = {ccy: amount}
        return copy.deepcopy(self.equity)
    
    def get_start_adjEq(self):
        self.get_equity() if not hasattr(self, "euqity") else None
        self.liability = 0.0
        ccy = list(self.equity.keys())[0].upper()
        price = 1 if ccy in ["USDT", "USDC", "BUSD", "USDC", "USDK", "DAI"] else self.get_redis_price(coin = ccy, suffix = "usdt")
        self.start_adjEq = price * self.equity[ccy]
    
    def get_upnl_master(self) -> dict[str, float]:
        upnl_master: dict[str, float] = {}
        self.get_now_position() if not hasattr(self, "amount_master") else None
        self.get_now_price_master() if not hasattr(self, "now_price_master") else None
        for coin in set(self.amount_master.keys()) & set(self.price_master.keys()):
            size = self.contract_master[coin] if coin in self.contract_master.keys() else self.get_contractsize_master(coin)
            upnl_master[coin] = size * self.amount_master[coin] * (1 / self.now_price_master[coin] - 1 / self.price_master[coin])
        self.upnl_master = upnl_master
        return upnl_master
    
    def get_upnl_slave(self) -> dict[str, float]:
        upnl_slave: dict[str, float] = {}
        self.get_now_price_slave() if not hasattr(self, "now_price_slave") else None
        for coin in set(self.amount_slave.keys()) & set(self.price_slave.keys()):
            upnl_slave[coin] = self.amount_slave[coin] * (self.price_slave[coin] - self.now_price_slave[coin])
        self.upnl_slave = upnl_slave
        return upnl_slave
    
    def get_upnl(self) -> tuple[dict[str, float], dict[str, float]]:
        upnl_master, upnl_slave = self.get_upnl_master(), self.get_upnl_slave()
        return upnl_master, upnl_slave
    
    def get_disacount_adjEq(self) -> float:
        adjEq = self.start_adjEq
        return adjEq
    
    def get_mmr(self) -> tuple[dict[str, float], dict[str, float]]:
        mmr_master: dict[str, float] = {}
        for coin, amount in self.amount_master.items():
            mmr_master[coin] = self.get_mmr_master(coin = coin, amount = abs(amount))
        mmr_slave: dict[str, float] = {}
        for coin, amount in self.amount_slave.items():
            mmr_slave[coin] = self.get_mmr_slave(coin = coin, amount = abs(amount))
        self.mmr_master, self.mmr_slave = mmr_master, mmr_slave
        return mmr_master, mmr_slave
    
    def cal_mm_master(self) -> dict[str, float]:
        mm_master = {}
        for coin, mmr in self.mmr_master.items():
            mm_master[coin] = mmr * abs(self.amount_master[coin]) * self.contract_master[coin]
        return mm_master
    
    def get_usd_upnl(self, coin: str):
        upnl = {coin: sum(self.upnl_slave.values())}
        tier = self.tier_upnl[coin] if coin in self.tier_upnl.keys() else self.get_tier_upnl(coin = coin)
        mmr = {coin: self.find_mmr(amount = upnl[coin], tier = tier)}
        mm ={coin: mmr[coin] * upnl[coin]}
        self.mm_upnl.update(mm)
        self.mmr_upnl.update(mmr)
    
    def cal_mm_upnl(self) -> dict[str, float]:
        mm_upnl = {}
        mmr_upnl = {}
        for coin, amount in self.upnl_master.items():
            amount -= self.equity[coin] if coin in self.equity.keys() else 0
            tier = self.tier_upnl[coin] if coin in self.tier_upnl.keys() else self.get_tier_upnl(coin)
            mmr_upnl[coin] = self.find_mmr(amount = amount, tier = tier)
            price = self.now_price_master[coin] if coin in self.now_price_master.keys() else self.get_spot_price(coin)
            mm_upnl[coin] = mmr_upnl[coin] * price * self.upnl_master[coin]
        self.mm_upnl, self.mmr_upnl = mm_upnl, mmr_upnl
        return copy.deepcopy(self.mm_upnl)
    
    def cal_mm(self) -> tuple[dict[str, float], dict[str, float]]:
        self.get_mmr()
        self.mm_master, self.mm_slave = self.cal_mm_master(), self.cal_mm_slave()
        self.cal_mm_upnl()
        self.get_usd_upnl(coin = self.slave.split("_")[0].upper())
        return copy.deepcopy(self.mm_master), copy.deepcopy(self.mm_slave), copy.deepcopy(self.mm_upnl)
    
    def cal_mr(self) -> float:
        self.get_start_adjEq() if not hasattr(self, "start_adjEq") else None
        self.get_upnl()
        adjEq = self.get_disacount_adjEq()
        mm_master, mm_slave, mm_upnl = self.cal_mm()
        self.get_fee_mm()
        mm = (sum(mm_master.values()) + sum(mm_slave.values()) + sum(mm_upnl.values()) + self.fee_mm)
        mr:float = adjEq / mm if mm != 0 else np.nan
        self.mr = mr
        self.real_adjEq = adjEq
        return mr
    
    def get_now_position(self):
        super().get_now_position()
        coins = list(self.amount_slave.keys())
        for coin in coins:
            amount = self.amount_slave[coin]
            size = self.contract_master[coin] if coin in self.contract_master.keys() else self.get_contractsize_master(coin)
            if not np.isnan(size):
                self.amount_master[coin] = - round(amount * self.price_slave[coin] / size, 0)
            else:
                del self.amount_master[coin]
                del self.amount_slave[coin]
    
    def get_account_position(self) -> pd.DataFrame:
        account_position = super().get_account_position()
        for i in account_position.index:
            coin = account_position.loc[i, "coin"]
            size = self.contract_master[coin] if coin in self.contract_master.keys() else self.get_contractsize_master(coin)
            account_position.loc[i, 'mv'] = abs(self.amount_master[coin]) * size
        account_position["mv%"] = round(account_position["mv"] / self.adjEq * 100, 4)
        self.account_position = account_position.copy()
        return account_position