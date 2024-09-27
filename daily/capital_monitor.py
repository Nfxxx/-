from research.utils import readData
from research.utils.ObjectDataType import AccountData
from cr_monitor.daily.daily_monitor import DailyMonitorDTO
import yaml, os, json, requests, datetime, copy, logging, traceback
from cr_monitor.connect.announcement import ExchangeAnnouncement

class CapitalMonitor(DailyMonitorDTO):
    def __init__(self, log_path = os.environ['HOME'] + "/data/cr_monitor"):
        super().__init__()
        self.log_path = log_path
        self.load_dingding()
        self.announ = ExchangeAnnouncement()
        self.load_combo_people()
        self.load_logger()
        self.warning_hedge = {"value": 50, "amount": 10}
        self.warning_rebalance = 1
        self.warning_account = {"ssh": [],
                                "yyz": [],
                                "scq": []}
    
    def load_logger(self):
        Log_Format = "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
        today = str(datetime.date.today()) 
        path = self.log_path
        path_save = f"{path}/logs/"
        
        if not os.path.exists(path_save):
            os.makedirs(path_save)
        file_name = f"{path_save}{today}.log"
        logger = logging.getLogger(__name__)
        logger.setLevel(level = logging.DEBUG)
        handler = logging.FileHandler(filename=file_name, encoding= "UTF-8")
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(Log_Format)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        self.logger = logger
    
    def load_combo_people(self):
        combo_people = {
            "okx_usd_swap-okx_usdt_swap": "ssh",
            'okx_usd_future-okx_usdt_swap': "ssh",
            'okx_usdt_swap-okx_usd_future': "ssh",
            'okx_spot-okx_usdt_swap': "ssh",
            "gate_spot-gate_usdt_swap": "yyz",
            "gate_usdt_swap-okx_usdt_swap": "yyz",
            "binance_usdc_swap-binance_usdt_swap": "yyz",
            "binance_busd_swap-binance_usdt_swap": "scq",
            'binance_usd_swap-binance_usdt_swap': "scq"
        }
        for account in self.accounts.values():
            if account.combo not in combo_people.keys():
                combo_people[account.combo] = "ssh"
        self.combo_people = combo_people
    
    def get_exchange_people(self) -> None:
        exchange_people = {"okex": [], "binance": [], "gate": []}
        for combo, people in self.combo_people.items():
            if "okx" in combo:
                exchange_people["okex"].append(people)
            else:
                pass
            for exchange in ["binance", "gate"]:
                if exchange in combo:
                    exchange_people[exchange].append(people)
                else:
                    pass
        self.exchange_people = exchange_people
        self.logger.info(f"exchange people: {exchange_people}")
    
    def load_dingding(self) -> None:
        user_path = os.path.expanduser('~')
        cfg_path = os.path.join(user_path, '.dingding')
        with open(os.path.join(cfg_path, 'key.yml')) as f:
            key = yaml.load(f, Loader = yaml.SafeLoader)
        self.dingding_config = key[1]
    
    def init_accounts(self) -> None:
        #初始化账户
        deploy_ids = self.get_all_deploys()
        accounts = {}
        for deploy_id in deploy_ids:
            parameter_name, strategy = deploy_id.split("@")
            client, username = parameter_name.split("_")
            if client not in ["test", "lxy"]:
                #只监控实盘账户
                if "h3f_binance_uswap_binance_uswap" not in strategy:
                    master, slave, ccy = self.get_strategy_info(strategy)
                else:
                    master, slave, ccy = self.get_bbu_info(strategy)
                accounts[parameter_name] = AccountData(
                    username = username,
                    client = client,
                    parameter_name = parameter_name,
                    master = master,
                    slave = slave,
                    principal_currency = ccy,
                    strategy = "funding", 
                    deploy_id = deploy_id)
            else:
                pass
        self.accounts = accounts.copy()
    
    def get_coins(self) -> None:
        """获得数据库里面所有需要对冲币种的名称"""
        a = """SHOW FIELD KEYS FROM "pnl_hedge" """
        data = readData.read_influx(a, db = "ephemeral")
        coins = list(data["fieldKey"].values)
        self.coins = coins
    
    def get_coins_str(self) -> None:
        if not hasattr(self, "coins"):
            self.get_coins()
        else:
            pass
        coins_str = ''
        for coin in self.coins:
            coins_str = coins_str + '"' + coin + '"' +","
        self.coins_str = coins_str[:-1]
        self.logger.info(f"coins str: {self.coins_str}")
    
    def get_cashbalance(self, account):
        a = f"""select {self.coins_str} from "pnl_hedge" where deploy_id = '{account.deploy_id}' and time > now() - 1h """
        data = readData.read_influx(a, db = "ephemeral", transfer= False)
        data.dropna(how = "all", axis = 1, inplace= True)
        data.fillna(0, inplace= True)
        cols = set(self.coins) & set(data.columns)
        for coin in cols:
            if coin not in ["usdt", "usd", "busd", "usdc"]:
                price = account.get_coin_price(coin = coin)
            else:
                price = 1
            data[coin] = abs(data[coin]) * price
        data["total"] = data[list(cols)].sum(axis = 1)
        account.cashbalance = data.copy()
    
    def send_dingding(self, data: dict):
        url = self.dingding_config["url"]
        header = {
            "Content-Type": "application/json",
            "Charset": "UTF-8"
        }
        send_data = json.dumps(data)
        send_data = send_data.encode("utf-8")
        ret = requests.post(url = url, data = send_data, headers= header)
    
    def run_monitor_pnl(self):
        self.get_coins_str()
        warning_account = copy.deepcopy(self.warning_account)
        for name, account in self.accounts.items():
            self.get_cashbalance(account)
            self.logger.info(f"cashbalance of {account.parameter_name}: {account.cashbalance}")
            amount_plus = len(account.cashbalance[account.cashbalance["total"] > self.warning_hedge["value"]])
            if amount_plus > self.warning_hedge["value"]:
                warning_people = self.combo_people[account.combo]
                warning_account[warning_people].append(name)
            else:
                pass
        #发送警告
        for people in warning_account.keys():
            number = len(warning_account[people])
            if number > 0:
                data = {
                    "msgtype": "text",
                    "text": {"content": f"""[AM]-[CashBalanceWarning] \n {warning_account[people]} CashBalance 过去1h对冲超过{self.warning_hedge["value"]}的次数超过{self.warning_hedge["amount"]}次\n时间：{datetime.datetime.now()}"""},
                    "at": {
                        "atMobiles": [self.dingding_config[people]],
                        "isAtAll": False}
                }
                self.send_dingding(data)
            else:
                data = {}
            self.logger.info(f"run monitor pnl send info: {data}")
    
    def find_af_accounts(self) -> None:
        """找到跨所账户"""
        af_accounts = {}
        for name, account in self.accounts.items():
            if account.exchange_master != account.exchange_slave:
                af_accounts[name] = account
            else:
                pass
        self.af_accounts = af_accounts
        self.logger.info(f"af account: {af_accounts}")
    
    def load_af_config(self) -> None:
        """设置转账config"""
        data = {"balance": {"okx_usdt_swap-binance_usdt_swap": 1,
                            "okx_usd_swap-binance_usdt_swap": 1,
                            "gate_usdt_swap-okx_usdt_swap": 1},
            "balance_limit": 0.2}
        self.af_config = data
        self.logger.info(f"af config: {data}")
    
    def get_bilateral_assets(self, account) -> None:
        #获得两边的资产
        names = {"master": account.exchange_master, "slave": account.exchange_slave}
        assets = {}
        
        for key, exchange in names.items():
            if exchange in ["okex", "okx", "ok", "okex5", "o"]:
                exchange_name = "okexv5"
            else:
                exchange_name = exchange
            a = f"""
            SELECT last(usdt) as adjEq FROM "balance_v2" WHERE time > now() - 10m and username = '{account.username}' and client = '{account.client}' and exchange = '{exchange_name}' and balance_id != '{account.balance_id}'
            """
            data = readData.read_influx(a, transfer = False)
            assets[key] = float(data["adjEq"].values)
        account.assets = assets
    
    def get_people_coins(self) -> None:
        people_coins = {
            "ssh": set(["all"]),
            "brad": set(["all"]),
            "yyz": set(["all"]),
            "scq": set(["all"])
        }
        for account in self.accounts.values():
            if not hasattr(account, "now_position"):
                now_position = account.get_now_position()
                
            else:
                now_position = account.now_position
            self.logger.info(f"get {account.parameter_name} position")
            coins = set(now_position.index.values)
            people = self.combo_people[account.combo]
            people_coins[people] = people_coins[people] | coins
        self.people_coins = people_coins
        self.logger.info(f"people coins: {people_coins}")
                    
    def run_monitor_assets(self):
        self.load_af_config()
        self.find_af_accounts()
        self.balance_limit = self.af_config["balance_limit"]
        warning_account = copy.deepcopy(self.warning_account)
        for name, account in self.af_accounts.items():
            self.get_bilateral_assets(account)
            balance_ratio = self.af_config["balance"][account.combo]
            if abs(account.assets["master"] / (balance_ratio * account.assets["slave"]) - 1) > self.warning_rebalance * self.balance_limit:
                warning_people = self.combo_people[account.combo]
                warning_account[warning_people].append(name)
            else:
                pass
        #发送警告
        for people in warning_account.keys():
            number = len(warning_account[people])
            if number > 0:
                data = {
                    "msgtype": "text",
                    "text": {"content": f"""[AM]-[RebalanceWarning] \n {warning_account[people]} 两边资金相差过大！\n时间：{datetime.datetime.now()}"""},
                    "at": {
                        "atMobiles": [self.dingding_config[people]],
                        "isAtAll": False}
                }
                self.send_dingding(data)
            else:
                data = {}
            self.logger.info(f"run monitor assests send info: {data}")
        
    def run_monitor_delist(self):
        self.get_exchange_people()
        self.get_people_coins()
        self.announ.get_delist_coins()
        delist_coins = self.announ.delist_coins
        self.logger.info(f"delist coins: {delist_coins}")
        for exchange, coins in delist_coins.items():
            if len(coins) > 0:
                mobiles = []
                for people in self.exchange_people[exchange]:
                    if len(coins & self.people_coins[people]) > 0:
                        mobiles.append(self.dingding_config[people])
                    else:
                        pass
                if mobiles != []:
                    data = {
                        "msgtype": "text",
                        "text": {"content": f"""[AM]-[DelistWarning] \n {exchange} 的 {coins} 近期有下架公告 {self.announ.url_config[exchange]}\n时间：{datetime.datetime.now()}"""},
                        "at": {
                            "atMobiles": mobiles,
                            "isAtAll": False}
                    }
                    self.send_dingding(data)
                else:
                    data = {}
                self.logger.info(f"delist coins send info: {data}")
    
    def run_monitor(self):
        self.logger.info("Start !!!!!!!!!!!!!!")
        try:
            if True:
            # if datetime.datetime.utcnow().hour == 2:
                self.run_monitor_delist()
            self.run_monitor_assets()
            self.run_monitor_pnl()
            self.logger.info("End !!!!!!!!!!!!!!!")
        except Exception as e:
            self.logger.critical(e)
            self.logger.critical(traceback.format_exc())
        self.logger.handlers.clear()

