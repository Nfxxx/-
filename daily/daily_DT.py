from cr_monitor.daily.daily_DTC import DailyDTC
from cr_assis.connect.connectData import ConnectData
from cr_assis.pnl.dtcPnl import DtcPnl
from cr_monitor.position.Position_DT import PositionDT
from cr_monitor.mr.Mr_DT import MrDT
import pandas as pd
from research.eva import eva
import datetime, copy
from cr_monitor.daily.daily_monitor import set_color, set_funding_color

class DailyDT(DailyDTC):
    
    def __init__(self, ignore_test=True):
        self.ignore_test = ignore_test
        self.database = ConnectData()
        self.position = PositionDT
        self.mr = MrDT
        self.strategy_name = "dt_okex_cswap_okex_uswap_btc"
        self.combo = 'okx_usd_swap-okx_usdt_swap'
        self.init_accounts()
        self.get_pnl_daily = DtcPnl(accounts = list(self.accounts.values()))
    
    def get_chance(self):
        eva.run_funding("okex", "usdt", "okex", "usd", datetime.date(2021,1,1), datetime.date.today(), play = True, input_coins=["BTC", "ETC", "XRP"])
        self.funding_summary, self.funding, _ = eva.run_funding("okex", "usdt", "okex", "usd", datetime.date.today() + datetime.timedelta(days = -33), datetime.date.today(), play = False)
        self.funding_summary.drop(["last_dt", "1t"], inplace = True, axis = 1)
        self.funding_summary.dropna(subset = ["1d", "volume_U_24h"], inplace = True)
        self.funding_summary.rename(columns = {"volume_U_24h": "vol_24h"}, inplace = True)
        coins = list(self.funding_summary.index)
        coins.remove("BTC")
        coins.remove("ETH")
        self.funding_summary = self.funding_summary.loc[["BTC", "ETH"] + coins]
        for col in ["1d", "3d", "7d", "15d", "30d"]:
            num = int(col.split("d")[0]) * 3
            self.funding_summary[col + "_avg"] = self.funding_summary[col] / num
        self.funding = self.funding.T
        rate = pd.DataFrame(columns = ["next", "current"])
        for coin in self.funding_summary.index:
            data0 = eva.get_last_influx_funding(exchange_name="okex", pair_name=f"{coin.lower()}-usdt-swap")
            data1 = eva.get_last_influx_funding(exchange_name="okex", pair_name=f"{coin.lower()}-usd-swap")
            rate.loc[coin] = [data0["next_fee"].values[-1] - data1["next_fee"].values[-1], data0["rate"].values[-1] - data1["rate"].values[-1]]
        self.funding_summary = pd.merge(rate, self.funding_summary, left_index = True, right_index = True, how = "outer")
        result = copy.deepcopy(self.funding_summary)
        format_dict = {}
        for col in result.columns:
            if col != "vol_24h":
                format_dict[col] = '{0:.3%}'
            else:
                format_dict[col] = lambda x: format(round(x, 0), ",")
        funding_summary = result.style.applymap(set_funding_color).format(format_dict)
        return funding_summary