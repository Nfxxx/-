import datetime, ccxt, os, yaml, requests
import pandas as pd
import numpy as np
from cr_assis.draw import draw_ssh
from bokeh.plotting import figure, show
from bokeh.models.widgets import Panel, Tabs

class MrDTO(object):
    
    """calucate MarginRatio for DT-O. 
    DT means the holding position only includes usdt-swap and usd-swap, and the 'o' means they are both in okex.
    position and asset ccy is the same."""
    
    def __init__(self, amount_u: int, amount_c: int, amount_fund: float, price_u: float, price_c: float, now_price: float, coin = "BTC", is_long = True):
        """amount_u: the amount of USDT swap contracts(not coins) in holding position
        amount_c: the amount of USD swap contracts(not dollars) in holding position
        amount_fund: the amount of asset 
        price_u: the average price of USDT swap in holding position
        price_c: the average price of USD swap in holding position
        now_price: the price of coin, now
        coin: the assest and position currency
        is_long: the direction of position. "long" means long c_swap and short u_swap, while "short" means short c_swap and long u_swap"""
        self.amount = {"usdt": amount_u, "usd": amount_c}
        self.api_url = 'https://www.okex.com/api/v5/public/position-tiers'
        self.holding_price = {"usdt": price_u, "usd": price_c}
        self.now_price = now_price
        self.amount_fund = amount_fund
        self.is_long = is_long
        self.coin = coin.upper()
        self.contract = self.get_contractsize() # the contractSize of coin
        self.coin_u = self.contract["usdt"] * self.amount["usdt"] # the amount of USDT swap coins in holding position
        self.value_c = self.contract["usd"] * self.amount["usd"] # the market value of USD swap in holding position

    def initialize(self) -> None:
        """initialize account MarginRatio
        """
        self.swap_tier = self.get_swap_tier()
        self.spot_tier = self.get_spot_tier()
        self.mmr_swap = self.get_mmr_swap()
        self.upnl = self.get_upnl(now_price = self.now_price)
        self.mainten_swap, self.mainten_spot = self.get_maintenance(now_price = self.now_price)
        self.mr = self.get_mr(now_price = self.now_price)
        
    def load_okex_key(self) -> None:
        user_path = os.path.expanduser('~')
        cfg_path = os.path.join(user_path, '.mr_dto')
        if not os.path.exists(cfg_path):
            os.mkdir(cfg_path)
        with open(os.path.join(cfg_path, 'okex.yml')) as f:
            self.key = yaml.load(f, Loader = yaml.SafeLoader)[0]
        
        
    def get_contractsize(self) -> dict:
        """get contractsize of this coin"""
        exchange = ccxt.okex()
        markets = exchange.load_markets()
        contract = {}
        contract["usdt"] = markets[f"{self.coin}/USDT:USDT"]["contractSize"]
        contract["usd"] = markets[f"{self.coin}/USD:{self.coin}"]["contractSize"]
        return contract

    def handle_origin_tier(self, data: list) -> pd.DataFrame:
        """"data" is the origin data return from okex api"""
        tiers = pd.DataFrame(columns = ["minSz", "maxSz", "mmr", "imr", "maxLever"])
        for i in range(len(data)):
            for col in tiers.columns:
                tiers.loc[i, col] = eval(data[i][col])
        return tiers
    
    
    def get_tier(self, instType, tdMode, instFamily=None, instId=None, tier=None, ccy = None):
        params = {k:v  for k, v in locals().items() if k != 'self' and v is not None}
        url = self.parse_params_to_str(params)
        ret = requests.get(self.api_url+url)
        return ret.json()
    
    def parse_params_to_str(self, params):
        url = '?'
        for key, value in params.items():
            url = url + str(key) + '=' + str(value) + '&'
        return url[0:-1]
    
    def get_swap_tier(self) -> dict:
        swap_tier = {}
        for suffix in ["usdt", "usd"]:
            name = f"{self.coin}-{suffix.upper()}"
            data = self.get_tier(instType = "SWAP", 
                    tdMode = "cross",
                    instFamily= name,
                    instId= name,
                    tier="")["data"]
            tier = self.handle_origin_tier(data)
            swap_tier[suffix] = tier.copy()
        return swap_tier
    
    def get_spot_tier(self) -> dict:
        spot_tier = {}
        for ccy in [self.coin, "USDT"]:
            ret = self.get_tier(instType = "MARGIN", 
                tdMode = "cross",
                ccy = ccy)
            # print(ret)
            data = ret["data"]
            tier = self.handle_origin_tier(data)
            spot_tier[ccy] = tier.copy()
        return spot_tier
            
    def get_mmr_swap(self) -> dict:
        """get swap maintenance margin ratio
        Returns:
            dict: maintenance margin ratio of swap
        """
        #
        mmr_swap = {}
        for contract in ["usdt", "usd"]:
            tier = self.swap_tier[contract]
            amount = self.amount[contract]
            for i in tier.index:
                if amount >= tier.loc[i, "minSz"] and amount <= tier.loc[i, "maxSz"]:
                    mmr_swap[contract] = tier.loc[i, "mmr"]
                    break
                else:
                    pass
        return mmr_swap
    
    def get_mmr_spot(self, amount: float, ccy: str) -> float:
        """get spot maintenance margin ratio

        Args:
            amount (float): the number of negative upnl coin
            ccy (str): the name of negative upnl coin

        Returns:
            float: maintenance margin ratio of spot
        """
        tier = self.spot_tier[ccy]
        mmr = 0
        if amount > 0:
            for i in tier.index:
                if amount > tier.loc[i, "minSz"] and amount <= tier.loc[i, "maxSz"]:
                    mmr = tier.loc[i, "mmr"]
                    break
                else:
                    pass
        else:
            pass
        if amount > max(tier["maxSz"].values):
            mmr = float(tier[tier["maxSz"] == max(tier["maxSz"].values)]["mmr"].values)
        return mmr 
    
    def get_upnl(self, now_price: float) -> dict:
        """
        Args:
            now_price (float): now price of coin, or a assumed price

        Returns:
            dict: the amount of coin, not dollars. upnl = {"USDT": float, self.coin: float}
        """
        upnl = {"USDT": 0, self.coin: 0} #
        if self.is_long:
            upnl[self.coin] = self.value_c / self.holding_price["usd"] - self.value_c / now_price + self.amount_fund
            upnl["USDT"] = (self.holding_price["usdt"] - now_price) * self.coin_u
        else:
            upnl[self.coin] = self.value_c / now_price - self.value_c / self.holding_price["usd"] + self.amount_fund
            upnl["USDT"] = (now_price - self.holding_price["usdt"]) * self.coin_u
        return upnl
    
    def get_upnl_spread(self, spread: float) -> dict:
        """
        Args:
            spread (float): assumed spread

        Returns:
            dict: the amount of coin, not dollars. upnl = {"USDT": float, self.coin: float}
        """
        now_price = self.now_price
        upnl = {"USDT": 0, self.coin: 0} 
        if self.is_long:
            price_u = now_price * (1+spread)
            upnl[self.coin] = self.value_c / self.holding_price["usd"] - self.value_c / now_price + self.amount_fund
            upnl["USDT"] = (self.holding_price["usdt"] - price_u) * self.coin_u
        else:
            price_u = now_price * (1-spread)
            upnl[self.coin] = self.value_c / self.holding_price["usd"] - self.value_c / now_price + self.amount_fund
            upnl["USDT"] = (self.holding_price["usdt"] - price_u) * self.coin_u
        return upnl
    
    def get_maintenance(self, now_price: float, spread = None) -> tuple([dict, dict]):
        # get maintenance margin value, including swap and spot
        #now_price: now price of coin, or a assumed price
        mainten_swap = {"usdt": 0, "usd": 0}
        mainten_swap["usd"] = self.value_c * self.mmr_swap["usd"]
        mainten_swap["usdt"] = self.coin_u * now_price * self.mmr_swap["usdt"]
        mainten_spot = {self.coin: 0, "USDT": 0}
        price = {self.coin: now_price, "USDT": 1}
        if spread  == None:
            upnl = self.get_upnl(now_price = now_price)
        else:
            upnl = self.get_upnl_spread(spread = spread)
        for ccy in [self.coin, "USDT"]:
            mmr = self.get_mmr_spot(amount = -upnl[ccy], ccy = ccy)
            mainten_spot[ccy] = mmr * abs(upnl[ccy]) * price[ccy]
        return mainten_swap, mainten_spot
    
    def get_mr(self, now_price: float) -> float:
        #now_price: now price of coin, or a assumed price
        fund_value = self.amount_fund * now_price
        mainten_swap, mainten_spot = self.get_maintenance(now_price = now_price)
        total_mainten = sum(mainten_swap.values()) + sum(mainten_spot.values()) + (self.value_c + self.coin_u * self.now_price) * 0.0008
        mr = fund_value / total_mainten
        return mr
    
    def coin_value_influence(self) -> pd.DataFrame:
        price_min = min(self.now_price * 0.5, 4000)
        price_max = self.now_price * 1.5
        result = pd.DataFrame(columns = ["mr"])
        for price in np.linspace(price_min, price_max, num = 25):
            result.loc[price, "mr"] = self.get_mr(price)
        return result
    
    def get_spread_influence(self) -> pd.DataFrame:
        spreads = np.linspace(0, 0.5, num = 25)
        result = pd.DataFrame(columns = ["mr"])
        for spread in spreads:
            fund_value = self.amount_fund * self.now_price - spread * self.value_c
            mainten_swap, mainten_spot = self.get_maintenance(now_price = self.now_price, spread = spread)
            total_mainten = sum(mainten_swap.values()) + sum(mainten_spot.values()) + (self.value_c + self.coin_u * self.now_price) * 0.0008
            mr = fund_value / total_mainten
            result.loc[spread, "mr"] = mr
        return result
    
    def run_mmr(self, play = True, title = "") -> None:
        self.initialize()
        self.value_influence = self.coin_value_influence()
        self.spread_influence = self.get_spread_influence()
        if play:
            result = self.value_influence.copy()
            result["MarginCall"] = 3
            result["LimitClose"] = 6
            p1 = draw_ssh.line(result, x_axis_type = "linear", play = False, title = title,
                            x_axis_label = "coin price", y_axis_label = "mr")
            tab1 = Panel(child=p1, title="value influence")
            result = self.spread_influence.copy()
            result["MarginCall"] = 3
            result["LimitClose"] = 6
            p2 = draw_ssh.line(result, x_axis_type = "linear", play = False, title = title,
                            x_axis_label = "spread", y_axis_label = "mr")
            tab2 = Panel(child=p2, title="spread influence")
            tabs = Tabs(tabs=[tab1, tab2])
            show(tabs)