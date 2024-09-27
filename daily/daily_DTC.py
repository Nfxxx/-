from cr_monitor.daily.daily_SSFO import DailySSFO
from cr_assis.connect.connectData import ConnectData
from cr_assis.pnl.dtcPnl import DtcPnl
from cr_monitor.position.Position_DTC import PositionDTC
import pandas as pd
from research.eva import eva
import datetime, copy
from cr_monitor.mr.Mr_DTC import MrDTC
from cr_monitor.daily.daily_monitor import set_color, set_funding_color

class DailyDTC(DailySSFO):
    """DTC means master is usd_swap and slave is usdc_swap
    """
    
    def __init__(self, ignore_test=True):
        self.ignore_test = ignore_test
        self.database = ConnectData()
        self.position = PositionDTC
        self.mr = MrDTC
        self.strategy_name = "dt_okex_cswap_okex_uswap_btc"
        self.combo = 'okx_usd_swap-okx_usdc_swap'
        self.init_accounts()
        self.get_pnl_daily = DtcPnl(accounts = list(self.accounts.values()))
        
    def get_chance(self):
        self.funding_summary, self.funding, _ = eva.run_funding("okex", "usdc", "okex", "usd", datetime.date(2022,10,1), datetime.date.today(), play = True)
        self.funding_summary.drop(["last_dt", "1t"], inplace = True, axis = 1)
        self.funding_summary.dropna(subset = ["1d"], inplace = True)
        self.funding_summary.rename(columns = {"volume_U_24h": "vol_24h"}, inplace = True)
        for col in ["1d", "3d", "7d", "15d", "30d"]:
            num = int(col.split("d")[0]) * 3
            self.funding_summary[col + "_avg"] = self.funding_summary[col] / num
        self.funding = self.funding.T
        self.get_now_situation() if not hasattr(self, "now_situation") else None
        rate = pd.DataFrame(columns = ["next", "current"])
        for coin in self.funding_summary.index:
            data0 = eva.get_last_influx_funding(exchange_name="okex", pair_name=f"{coin.lower()}-usdc-swap")
            data1 = eva.get_last_influx_funding(exchange_name="okex", pair_name=f"{coin.lower()}-usd-swap")
            rate.loc[coin] = [data0["next_fee"].values[-1] - data1["next_fee"].values[-1], data0["rate"].values[-1] - data1["rate"].values[-1]]
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
        funding_summary = result.style.applymap(set_funding_color).format(format_dict)
        return funding_summary