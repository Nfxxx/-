from cr_assis.account.accountOkex import AccountOkex
from cr_monitor.daily.dailyOkex import DailyOkex
from cr_monitor.mr.mrOkex import MrOkex
import pandas as pd
import numpy as np
add = {
    "okx_spot-okx_usd_swap": {},
    "okx_spot-okx_usdt_swap":{"arb": 0.1, "fil": 0.2,"ltc": 0.2, "cfx": 0.1},
    "okx_spot-okx_usdc_swap":{"eth": 0.5},
    "okx_usd_swap-okx_usdt_swap":{}
}
# profit = pd.DataFrame().from_dict(data = {"beth": [0.7, 0.5],
#     "arb": [0.738, 0.1],
#     "pepe": [0.64, 0.1],
#     "fil": [0.745, 0.2],
#     "ltc": [0.777, 0.2],
#     "cfx": [0.8039, 0.1]}, orient="index", columns=["month_profit", "mv"])
# interest = 2
# profit["month_profit"] -= interest / 12
# profit["month"] = profit["month_profit"] * profit["mv"]
# profit["year"] = profit["month"] * 12
# print((profit["month"]).sum()/4)
# print((profit["month"]).sum())
# print((profit["year"]).sum())
# m = MrOkex()
# m.btc_num = [60]
# # m.price_range = [1]
# result = m.assumed_open(add)
# print(result)

m = MrOkex()
account = AccountOkex("wzok_002@pt_okex_btc")
m.btc_num = [0.2]
m.price_range = [1]
ret = m.run_account_mr(account, add={
    "okx_spot-okx_usdt_swap": {'ordi':0.1},
    "okx_usd_swap-okx_usdt_swap":{'btc':-0.3}
})
ret