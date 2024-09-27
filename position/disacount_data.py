
class DisacountData(object):
    def __init__(self) -> None:
        self.lv1 = {"coin":["ETH", "BTC", "TUSD", "USDT", "USDC"],
            "info": [{'discountRate': '1', 'maxAmt': '5000000', 'minAmt': '0'}, 
            {'discountRate': '0.975', 'maxAmt': '10000000', 'minAmt': '5000000'},
            {'discountRate': '0.975', 'maxAmt': '20000000', 'minAmt': '10000000'},
            {'discountRate': '0.95', 'maxAmt': '40000000', 'minAmt': '20000000'},
            {'discountRate': '0.9', 'maxAmt': '100000000', 'minAmt': '40000000'},
            {'discountRate': '0', 'maxAmt': '', 'minAmt': '100000000'}]}
        self.lv2 = {"coin":["EOS", "LTC", "BCH", "BETH"],
            "info": [{'discountRate': '0.95', 'maxAmt': '2000000', 'minAmt': '0'},
            {'discountRate': '0.85', 'maxAmt': '4000000', 'minAmt': '2000000'},
            {'discountRate': '0.5', 'maxAmt': '8000000', 'minAmt': '4000000'},
            {'discountRate': '0', 'maxAmt': '', 'minAmt': '8000000'}]
            }
        self.lv3 = {"coin": ["OKB", "DOT", "DOGE", "FIL", "SHIB", "ADA", "ATOM",
                    "AVAX", "BSV", "FTM", "LINK", "MANA", "MATIC", "SAND"],
            "info": [{'discountRate': '0.9', 'maxAmt': '1000000', 'minAmt': '0'},
            {'discountRate': '0.8', 'maxAmt': '2000000', 'minAmt': '1000000'},
            {'discountRate': '0.5', 'maxAmt': '4000000', 'minAmt': '2000000'},
            {'discountRate': '0', 'maxAmt': '', 'minAmt': '4000000'}]
            }
        self.lv4 = {
            "coin": ["AAVE", "ALGO", "GALA","BETH", "NEAR", "SOL", "SUSHI", "TRX", "UNI", "XRP"],
            "info": [{'discountRate': '0.85', 'maxAmt': '1000000', 'minAmt': '0'},
                {'discountRate': '0.75', 'maxAmt': '2000000', 'minAmt': '1000000'},
                {'discountRate': '0.5', 'maxAmt': '4000000', 'minAmt': '2000000'},
                {'discountRate': '0', 'maxAmt': '', 'minAmt': '4000000'}]
        }
        self.lv5 = {
            "coin": ["AXS", "BAT", "BNB", "COMP", "CRO", "CRV", "DASH", "EGLD",
            "ETC", "FLOW", "GRT", "ICP", "KSM", "LRC", "LUNA", "NEO", "OMG",
            "PEOPLE", "QTUM", "XLM", "XMR", "XTZ", "YFI", "YFII", "ZEC"],
            "info": [{'discountRate': '0.8', 'maxAmt': '250000', 'minAmt': '0'},
                {'discountRate': '0.7', 'maxAmt': '500000', 'minAmt': '250000'},
                {'discountRate': '0.5', 'maxAmt': '1000000', 'minAmt': '500000'},
                {'discountRate': '0', 'maxAmt': '', 'minAmt': '1000000'}]
        }
        self.lv6 = {
            "coin": ["CFX", "ARB", "1INCH", "AGLD", "AKITA", "ALPHA", "ANT", "APE",
            "API3", "APT", "AR", "BADGER", "BAL", "BAND", "BICO", "BLUR",
            "BNT", "CELR", "CELO", "CEL", "CHZ", "CORE", "CSPR", "CVC",
            "DORA", "DYDX", "ELF", "ENJ", "ENS", "ETHW", "FITFI", "FLM",
            "FLOKI", "GFT", "GLMR", "GMT", "GMX", "GODS", "HBAR", "IMX",
            "IOST", "IOTA", "JST", "KISHU", "KLAY", "KNC", "LAT", "LDO",
            "LOOKS", "LPT", "LUNC", "MAGIC", "MASK", "MINA", "MKR", "NFT",
            "ONT", "OP", "PERP", "REN", "RSR", "RSS3", "RVN", "SLP", "SNT",
            "SNX", "STARL", "STORJ", "STX", "SWEAT", "THETA", "TON", "TRB",
            "UMA", "USTC", "WAVES", "WOO", "XCH", "YGG", "ZEN", "ZIL", "ZRX", "PEPE", "AIDOGE", "SUI"],
            "info": [{'discountRate': '0.5', 'maxAmt': '50000', 'minAmt': '0'},
                {'discountRate': '0', 'maxAmt': '', 'minAmt': '50000'}]
        }