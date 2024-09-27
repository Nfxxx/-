from cr_assis.load import *
print(f"running time: {datetime.datetime.now()}")
import requests, yaml
from github import Github
from cr_assis.connect.connectOkex import ConnectOkex
from cr_assis.connect.connectData import ConnectData
database = ConnectData()
p = database.get_redis_data(key = f"okexv5/btc-usdt")
dataokex = ConnectOkex()
ret = dataokex.get_mmr(coin = "btc", amount = 10, contract="usdt-swap")
ret = dataokex.get_contractsize(coin = "btc", contract="usdt-swap")
ret = dataokex.get_tiers(coin = "btc", contract = "usdt-swap")


from imap_tools import MailBox
with open(f"{os.environ['HOME']}/.cr_assis/mongo_url.yml", "rb") as f:
    data = yaml.load(f, Loader= yaml.SafeLoader)
for info in data:
    if "name" in info.keys() and info["name"] == "gmail":
        email_account = info["address"]
        password = info["password"]
# get all attachments from INBOX and save them to files
with MailBox('imap.gmail.com').login(email_account, password, 'INBOX') as mailbox:
    for msg in mailbox.fetch():
        if msg.from_ == "coinrising111@outlook.com":
            for att in msg.attachments:
                with open('/Users/chelseyshao/Downloads/{}'.format(att.filename), 'wb') as f:
                    f.write(att.payload)

def find_mmr(amount: float, tier: pd.DataFrame) -> float:
        mmr = np.nan
        for i in tier.index:
            if amount > tier.loc[i, "minSz"] and amount <= tier.loc[i, "maxSz"]:
                mmr = tier.loc[i, "mmr"]
                break
        return mmr

tier = {}
contracts = ["ETH-USD", "ETH-USDC", "BTC-USD", "BTC-USDC"]
size = {"ETH-USD": 10, "ETH-USDC": 0.001, "BTC-USD": 100, "BTC-USDC": 0.0001}
for contract in contracts + ["USDC", "ETH"]:
    tier[contract] = pd.read_excel("/Users/ssh/Downloads/tier.xlsx", sheet_name=contract)
# for contract in contracts:
#     url = f"https://www.okx.com/api/v5/public/position-tiers?instType=SWAP&tdMode=cross&instFamily={contract}&instId={contract}&tier="
#     response = requests.get(url)
#     ret = response.json()
#     tier[contract] = pd.DataFrame(ret["data"].copy())
#     for col in ["minSz", "maxSz", "mmr"]:
#         tier[contract][col] = tier[contract][col].astype(float)
# for ccy in ["USDC", "ETH"]:
#     url = f"https://www.okx.com/api/v5/public/position-tiers?instType=MARGIN&tdMode=cross&ccy={ccy}&tier="
#     response = requests.get(url)
#     ret = response.json()
#     tier[ccy] = pd.DataFrame(ret["data"].copy())
#     for col in ["minSz", "maxSz", "mmr"]:
#         tier[ccy][col] = tier[ccy][col].astype(float)
num = 50
mul = 0.65
btc_price = 28081.6
eth_price = 1798.42
adjEq = num * btc_price
mv = adjEq * mul
mm = {}
mmr = {}
for contract in contracts:
    if "BTC-USDC" == contract:
        amount = num * mul / 0.0001
    elif contract == "ETH-USDC":
        amount = num * mul * btc_price / eth_price / 0.001
    else:
        amount = mv / size[contract]
    mmr[contract] = find_mmr(amount, tier[contract])
    mm[contract] = mmr[contract] * mv
mr = adjEq / sum(mm.values())
change_mr = pd.DataFrame()
for change in [0.5, 0.6, 0.7, 0.8, 0.9]:
    upnl = mv * 2 * (1 - change)
    mmr = find_mmr(amount = upnl, tier = tier["USDC"])
    change_mr.loc[change, num] = adjEq * change / (sum(mm.values()) + mmr * upnl)
for change in [1.1, 1.2, 1.3, 1.4, 1.5]:
    upnl = mv * (change - 1) / (eth_price*change)
    mmr = find_mmr(amount = upnl, tier = tier["ETH"])
    change_mr.loc[change, num] = adjEq * change / (sum(mm.values()) * change + mmr * upnl)
print(change_mr)
111


init_mr = pd.DataFrame()
for num in [30, 40, 50, 60]:
    for mul in [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]:
        adjEq = num * btc_price
        mv = adjEq * mul
        mm = {}
        mmr = {}
        for contract in contracts:
            if "BTC-USDC" == contract:
                amount = num * mul / 0.0001
            elif contract == "ETH-USDC":
                amount = num * mul * btc_price / eth_price / 0.001
            else:
                amount = mv / size[contract]
            mmr[contract] = find_mmr(amount, tier[contract])
            mm[contract] = mmr[contract] * mv
        mr = adjEq / sum(mm.values())
        init_mr.loc[mul, num] = mr


1111