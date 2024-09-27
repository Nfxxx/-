import ccxt, requests, copy
import pandas as pd
import numpy as np
from cr_assis.connect.connectData import ConnectData
from cr_monitor.position.disacount_data import DisacountData

class PositionSSFO(object):
    """Define the position information of SSFO"""
    
    def __init__(self, client = "", username = "") -> None:
        self.master:str = "spot"
        self.slave:str  = "usdt_swap"
        self.markets = ccxt.okex().load_markets()
        self.database = ConnectData()
        self.username:str  = username
        self.client:str  = client
        self.fee_rate = 0.002
        self.discount_data = DisacountData()
        self.equity:dict[str, float]# the asset
        self.tiers_url:str  = 'https://www.okex.com/api/v5/public/position-tiers'
        self.discount_url:str  = "https://www.okex.com/api/v5/public/discount-rate-interest-free-quota"
        self.discount_info:dict[str, float] = {}
        self.tier_slave: dict[str, float] = {}
        self.tier_master: dict[str, float] = {}
        self.contract_slave: dict[str, float] = {}
        self.amount_master:dict[str, float] # the amount of spot assets. A positive number means long while negative number means short.
        self.amount_slave:dict[str, float] # the amount of coins, not contracts
        self.price_master:dict[str, float] # the average open price of master asset
        self.price_slave:dict[str, float] # the average price of slave in holding position
        
    def get_contractsize_slave(self, coin: str) -> float:
        coin = coin.upper()
        contractsize = self.markets[f"{coin}/USDT:USDT"]["contractSize"]
        self.contract_slave[coin] = contractsize
        return contractsize
    
    def get_discount_info(self, coin: str) -> list:
        ret = []
        for name in ["lv1", "lv2", "lv3", "lv4", "lv5", "lv6"]:
            if coin in eval(f"self.discount_data.{name}")["coin"]:
                ret = eval(f"self.discount_data.{name}")["info"]
                break
        if ret == []:
            ret = self.get_discount_apiInfo(coin)
        return ret
    
    def get_discount_apiInfo(self, coin:str) -> list:
        """get discount information of special coin through api
        Args:
            coin (str): str.upper()
        Returns:
            list: list of dict
        """
        response = requests.get(self.discount_url + f"?ccy={coin.upper()}")
        if response.status_code == 200:
            ret = response.json()['data'][0]['discountInfo']
        else:
            ret = []
            print(response.json())
        self.discount_info[coin.upper()] = ret.copy()
        return ret
    
    def get_discount_asset(self, coin: str, asset: float) ->float:
        """
        Args:
            coin (str): coin name, str.upper()
            asset (float): the dollar value of coin
        """
        discount_info = self.get_discount_info(coin) if coin.upper() not in self.discount_info.keys() else self.discount_info[coin.upper()]
        if discount_info == []:
            real_asset = np.nan
        else:
            real_asset = 0.0
        for info in discount_info:
            minAmt = float(info["minAmt"]) if info["minAmt"] != "" else 0
            maxAmt = float(info["maxAmt"]) if info["maxAmt"] != "" else np.inf
            discountRate = float(info["discountRate"])
            if asset > minAmt and asset <= maxAmt:
                real_asset += (asset - minAmt) * discountRate
            elif asset > maxAmt:
                real_asset += maxAmt * discountRate
            else:
                break
        return real_asset
    
    def get_tier(self, instType, tdMode, instFamily=None, instId=None, tier=None, ccy = None) -> dict:
        params = {k:v  for k, v in locals().items() if k != 'self' and v is not None}
        url = self.parse_params_to_str(params)
        ret = requests.get(self.tiers_url+url)
        return ret.json()
    
    def parse_params_to_str(self, params: dict):
        url = '?'
        for key, value in params.items():
            url = url + str(key) + '=' + str(value) + '&'
        return url[0:-1]
    
    def handle_origin_tier(self, data: list) -> pd.DataFrame:
        """" data is the origin data return from okex api"""
        tiers = pd.DataFrame(columns = ["minSz", "maxSz", "mmr", "imr", "maxLever"])
        for i in range(len(data)):
            for col in tiers.columns:
                tiers.loc[i, col] = eval(data[i][col])
        return tiers
    
    def get_tier_swap(self, coin: str, contract: str) -> pd.DataFrame:
        name = name = f"{coin.upper()}-{contract}"
        data = self.get_tier(instType = "SWAP", 
                tdMode = "cross",
                instFamily= name,
                instId= name,
                tier="")["data"]
        tier = self.handle_origin_tier(data)
        return tier
    
    def get_tier_spot(self, coin: str) -> pd.DataFrame:
        ret = self.get_tier(instType = "MARGIN", 
            tdMode = "cross",
            ccy = coin.upper())
        tier = self.handle_origin_tier(ret["data"])
        return tier
    
    def get_tier_slave(self, coin: str) -> pd.DataFrame:
        tier = self.get_tier_swap(coin = coin, contract = "USDT")
        self.tier_slave[coin.upper()] = tier
        return tier
    
    def get_tier_master(self, coin: str) -> pd.DataFrame:
        tier = self.get_tier_spot(coin = coin)
        self.tier_master[coin.upper()] = tier
        return tier
    
    def find_mmr(self, amount: float, tier: pd.DataFrame) -> float:
        """
        Args:
            amount (float): the amount of spot asset or swap contract
            tier (pd.DataFrame): the position tier information
        """
        if amount <= 0:
            return 0
        else:
            mmr = np.nan
            for i in tier.index:
                if amount > tier.loc[i, "minSz"] and amount <= tier.loc[i, "maxSz"]:
                    mmr = tier.loc[i, "mmr"]
                    break
            return mmr
    
    def get_mmr_master(self, coin: str, amount: float) -> float:
        """get mmr of master, which is spot

        Args:
            coin (str): the name of coin, str.upper()
            amount (float): the number of spot asset, not dollar value
        """
        coin = coin.upper()
        tier = self.tier_master[coin] if coin in self.tier_master.keys() else self.get_tier_master(coin)
        mmr = self.find_mmr(amount = amount, tier = tier)
        return mmr
    
    def get_mmr_slave(self, coin: str, amount: float) -> float:
        """get mmr of slave, which is usdt_swap

        Args:
            coin (str): the name of coin, str.upper()
            amount (float): the coin number of usdt_swap asset, not dollar value, not contract number
        """
        coin = coin.upper()
        tier = self.tier_slave[coin] if coin in self.tier_slave.keys() else self.get_tier_slave(coin)
        contractsize = self.get_contractsize_slave(coin) if not coin in self.contract_slave.keys() else self.contract_slave[coin]
        num = amount / contractsize
        mmr = self.find_mmr(amount = num, tier = tier)
        return mmr
    
    def get_liability(self) -> float:
        self.liability = max(-self.get_cashBalance(ccy = "USDT"), 0)
        return copy.deepcopy(self.liability)
    
    def get_start_adjEq(self):
        self.get_equity() if not hasattr(self, "equity") else None
        self.get_now_position() if not hasattr(self, "amount_master") else None
        self.get_liability()
        ccy = list(self.equity.keys())[0].upper()
        price = 1 if ccy in ["USDT", "USDC", "BUSD", "USDC", "USDK", "DAI"] else self.get_master_price(ccy)
        start_adjEq = price * self.equity[ccy]
        self.start_adjEq = start_adjEq
        
    def get_upnl(self) -> dict[str, float]:
        upnl: dict[str, float] = {}
        self.get_now_price_slave() if not hasattr(self, "now_price_slave") else None
        for coin in set(self.amount_slave.keys()) & set(self.price_slave.keys()):
            upnl[coin] = (self.price_slave[coin] - self.now_price_slave[coin]) * self.amount_slave[coin]
        self.upnl = upnl
        return upnl
    
    def get_disacount_adjEq(self) -> float:
        self.get_now_price_master() if not hasattr(self, "now_price_master") else None
        coins = set(self.amount_master.keys()) & set(self.now_price_master.keys())
        adjEq = 0
        self.disacount_adjEq: dict[str, float] = {}
        for coin in coins:
            self.disacount_adjEq[coin]= self.get_discount_asset(coin = coin, asset = self.amount_master[coin] * self.now_price_master[coin])
        adjEq = self.start_adjEq - (self.liability + sum(self.upnl.values())) + sum(self.disacount_adjEq.values())
        return adjEq
    
    def get_now_price_master(self) -> dict[str, float]:
        now_price: dict[str, float] = {}
        for coin in set(self.amount_master.keys()):
            now_price[coin] = self.get_master_price(coin)
        self.now_price_master: dict[str, float] = now_price
        return now_price

    def get_mmr(self) -> tuple[dict[str, float], dict[str, float]]:
        mmr_master = {"USDT": self.get_mmr_master(coin = "USDT", 
                                                amount = self.liability + sum(self.upnl.values()) if list(self.equity.keys())[0] != "USDT" else max(self.equity["USDT"] - self.liability + sum(self.upnl.values()), 0))}
        mmr_slave: dict[str, float] = {}
        for coin, amount in self.amount_slave.items():
            mmr_slave[coin] = self.get_mmr_slave(coin = coin, amount = abs(amount))
        self.mmr_master, self.mmr_slave = mmr_master, mmr_slave
        return mmr_master, mmr_slave
    
    def get_now_price_slave(self) -> dict[str, float]:
        now_price_slave = {}
        for coin in set(self.amount_slave.keys()):
            now_price_slave[coin] = self.get_slave_price(coin)
        self.now_price_slave = now_price_slave
        return now_price_slave

    def cal_mm_master(self) -> dict[str, float]:
        mm_master = {"USDT": (self.liability + sum(self.upnl.values())) * self.mmr_master["USDT"]}
        return mm_master
    
    def cal_mm_slave(self) -> dict[str, float]:
        mm_slave = {}
        for coin, mmr in self.mmr_slave.items():
            mm_slave[coin] = mmr * abs(self.amount_slave[coin]) * self.now_price_slave[coin]
        return mm_slave

    def cal_mm(self) -> tuple[dict[str, float], dict[str, float]]:
        self.get_mmr()
        self.mm_master, self.mm_slave = self.cal_mm_master(), self.cal_mm_slave()
        return copy.deepcopy(self.mm_master), copy.deepcopy(self.mm_slave)
        
    def get_fee_mm(self) -> float:
        account_position = self.account_position if hasattr(self, "account_position") else self.get_account_position()
        mv = account_position["mv"].sum()
        self.fee_mm = mv * self.fee_rate
        return copy.deepcopy(self.fee_mm)

    def cal_mr(self) -> float:
        self.get_start_adjEq() if not hasattr(self, "start_adjEq") else None
        self.get_upnl()
        adjEq = self.get_disacount_adjEq()
        mm_master, mm_slave = self.cal_mm()
        self.get_fee_mm()
        mr:float = adjEq / (sum(mm_master.values()) + sum(mm_slave.values()) + self.fee_mm)
        self.mr = mr
        self.real_adjEq = adjEq
        return mr
    
    def get_origin_slave(self, start: str, end: str) -> dict[str, pd.DataFrame]:
        sql = f"""
        select ex_field, time, exchange, long, long_open_price, settlement, secret_id, last(short) as short, short_open_price, pair from position
        where client = '{self.client}' and username = '{self.username}' and time > {start} and time < {end} and (long >0 or short >0)
        and ex_field = 'swap' and settlement = 'usdt'
        group by time(1m), pair ORDER BY time
        """
        ret = self.database._send_influx_query(sql = sql, database = "account_data", is_dataFrame= False)
        result = {}
        for info in ret.keys():
            result[info[1]['pair']] = pd.DataFrame(ret[info]).dropna(subset = "secret_id")
        self.origin_slave:dict[str, pd.DataFrame] = result
        return result
    
    def load_redis_price(self):
        self.database.load_redis()
        self.redis_clt = self.database.redis_clt
    
    def get_redis_price(self, coin: str, suffix: str) -> float:
        self.load_redis_price() if not hasattr(self, "redis_clt") else None
        key = bytes(f"okexv5/{coin.lower()}-{suffix}", encoding="utf8")
        price = float(self.redis_clt.hgetall(key)[b'bid0_price']) if key in self.redis_clt.keys() else np.nan
        return price
    
    def get_master_price(self, coin: str) -> float:
        price = self.get_redis_price(coin = coin, suffix = "usdt")
        return price

    def get_slave_price(self, coin: str) -> float:
        price = self.get_redis_price(coin = coin, suffix = "usdt-swap")
        return price
    
    def get_slave_mv(self):
        for pair, data in self.origin_slave.items():
            coin = pair.split('-')[0]
            price = self.get_master_price(coin)
            data["mv"] = (data['short'] + data['long']) * price
    
    def get_now_position(self):
        ret = self.get_origin_slave(start = "now() - 5m", end = "now()")
        self.price_master: dict[str, float] = {}
        self.price_slave: dict[str, float] = {}
        self.amount_master: dict[str, float] = {}
        self.amount_slave: dict[str, float] = {}
        for pair, data in ret.items():
            coin = pair.split("-")[0].upper()
            data = data.dropna(subset = ["short", "long"])
            if len(data) >0 :
                self.amount_slave[coin] = (data["long"] - data["short"]).values[-1]
                self.price_slave[coin] = (data["short_open_price"] + data["long_open_price"]).values[-1]
        self.amount_master = {coin: -amount for coin, amount in self.amount_slave.items()}
        self.price_master = copy.deepcopy(self.price_slave)
    
    def get_cashBalance(self, ccy: str) -> float:
        sql = f"""
        SELECT last(origin) as origin FROM "equity_snapshot" 
        WHERE time > now() - 10m and username = '{self.username}' and client = '{self.client}'
        and symbol = '{ccy.lower()}'
        """
        ret = self.database._send_influx_query(sql, database = "account_data", is_dataFrame= True)
        amount = np.nan
        if len(ret) > 0:
            amount = float(eval(ret["origin"].values[-1])["cashBal"])
        return amount
    
    def get_equity(self, ccy = "BTC") -> dict[str, float]:
        amount = self.get_cashBalance(ccy = ccy)
        self.get_now_position() if not hasattr(self, "amount_master") else None
        if ccy in self.amount_master.keys():
            amount -= self.amount_master[ccy]
        self.equity = {ccy: amount}
        return copy.deepcopy(self.equity)
    
    def get_adjEq(self) -> float:
        sql = f"""
        select usdt, balance_id from balance_v2 where time > now() - 10m and client = '{self.client}' and username = '{self.username}'
        """
        ret = self.database._send_influx_query(sql, database = "account_data")
        ret.dropna(subset="balance_id", inplace = True)
        adjEq = np.nan
        if len(ret) > 0:
            adjEq = ret["usdt"].values[-1]
        self.adjEq = adjEq
        return adjEq
    
    def get_account_position(self) -> pd.DataFrame:
        self.get_now_position() if not hasattr(self, "amount_master") else None
        self.get_now_price_master() if not hasattr(self, "now_price_master") else None
        adjEq = self.get_adjEq() if not hasattr(self, "adjEq") else self.adjEq
        account_position = pd.DataFrame(columns = ["coin", "side", "position", "mv", "mv%"])
        num = 0
        for coin, amount in self.amount_master.items():
            side = "long" if amount > 0 else "short"
            mv = abs(amount) * self.now_price_master[coin]
            account_position.loc[num] = [coin, side, abs(amount), mv, round(mv / adjEq * 100, 4)]
            num += 1
        self.account_position = account_position.copy()
        return account_position
    
    def get_total_mv(self) -> float:
        ret = self.get_account_position() if not hasattr(self, "account_position") else self.account_position
        total_mv = ret["mv%"].sum()
        self.total_mv = total_mv
        return total_mv