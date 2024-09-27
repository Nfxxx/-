from cr_monitor.mr.Mr_DTO import MrDTO
import ccxt

class MrDTF(MrDTO):
    """calucate MarginRatio for DT-F, where master is usd-this_quarter and slave is usdt-swap"""
    
    def __init__(self, amount_u: int, amount_c: int, amount_fund: float, price_u: float, price_c: float, now_price: float, suffix: str, coin="BTC", is_long=False):
        "suffix: delivery date"
        self.suffix = suffix
        super().__init__(amount_u, amount_c, amount_fund, price_u, price_c, now_price, coin, is_long)
    
    def get_contractsize(self) -> dict:
        """get contractsize of this coin
        """
        exchange = ccxt.okex()
        markets = exchange.load_markets()
        contract = {}
        contract["usdt"] = markets[f"{self.coin}/USDT:USDT"]["contractSize"]
        contract['usd'] = markets[f"{self.coin}/USD:{self.coin}-{self.suffix}"]["contractSize"]
        return contract

    def get_swap_tier(self) -> dict:
        swap_tier = {}
        coin_contract = {"usdt": "SWAP", "usd": "FUTURES"}
        for suffix, contract in coin_contract.items():
            name = f"{self.coin}-{suffix.upper()}"
            data = self.get_tier(instType = contract, 
                    tdMode = "cross",
                    instFamily= name,
                    instId= name,
                    tier="")["data"]
            tier = self.handle_origin_tier(data)
            swap_tier[suffix] = tier.copy()
        return swap_tier
