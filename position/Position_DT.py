from cr_monitor.position.Position_DTC import PositionDTC
import pandas as pd

class PositionDT(PositionDTC):
    
    def __init__(self, client="", username="") -> None:
        super().__init__(client, username)
        self.slave = "usdt_swap"
    
    def get_contractsize_slave(self, coin: str) -> float:
        coin = coin.upper()
        contractsize = self.markets[f"{coin}/USDT:USDT"]["contractSize"]
        self.contract_slave[coin] = contractsize
        return contractsize