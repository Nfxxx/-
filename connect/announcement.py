import requests, copy
from scrapy.selector import Selector
import pandas as pd

class ExchangeAnnouncement(object):
    def __init__(self) -> None:
        self.url_config = {
            "binance": "https://www.binance.com/en/support/announcement/latest-binance-news?c=49&navId=49",
            "okex": "https://www.okx.com/support/hc/en-us/sections/360000030652-Latest-Announcements",
            "gate": "https://www.gate.io/articlelist/ann"
        }
        self.okex_announcement = {}
        self.binance_announcement = {}
        self.gate_announcement = {}
        self.keywords = ["delist", "remove"]
        
    def connect_announcement(self, url: str) -> bytes:
        ret = requests.get(url)
        body = ret._content
        return body

    def get_binance_announcement(self) -> list:
        url = self.url_config["binance"]
        body = self.connect_announcement(url)
        data = Selector(text=body).xpath('//script/text()').extract()
        data = data[10]
        null = "null"
        true = True
        false = False
        content = eval(data)
        content = content['routeProps']['ce50']['catalogs']
        articles = content[1]["articles"]
        data = []
        for i in range(len(articles)):
            data.append([articles[i]["title"]][0])
        self.binance_announcement = {"articles": articles, "title": data}
        return data

    def get_okex_announcement(self) -> list:
        url = self.url_config["okex"]
        body = self.connect_announcement(url)
        data = Selector(text=body).xpath('//a').extract()
        articles = []
        for info in data:
            if 'class="article-list-link"' in info:
                articles.append(info)
            else:
                pass
        data = []
        name = " data-monitor-name="
        for article in articles:
            str_list = article.split('"')
            if name in str_list:
                location = str_list.index(name)
                data.append(str_list[location + 1])
            else:
                pass
        self.okex_announcement = {"articles": articles, "title": data}
        return data
    
    def get_gate_announcement(self) -> list:
        url = self.url_config["gate"]
        body = self.connect_announcement(url)
        content = Selector(text=body).xpath('//a').extract()
        articles = []
        data = []
        for info in content:
            if 'href="/article' in info and 'title=' in info:
                articles.append(info)
                start_index = info.index("<h3>")
                end_index = info.index('</h3>')
                title = info[start_index+4: end_index]
                data.append(title)
            else:
                pass
        self.gate_announcement = {"articles": articles, "title": data}
        return data
    
    def get_words(self, title: str) -> list:
        title_name = copy.deepcopy(title)
        replace_char = [",", ".", "(", ")", "!", "?", "/"]
        for char in replace_char:
            title_name = title_name.replace(char, " ")
        words = title_name.split(" ")
        return words
    
    def get_real_token(self, word: str) -> str:
        stable_coins = ["USD", "USDT", "BUSD", "USDC", "USDK"]
        real_token = word
        for coin in stable_coins:
            num = len(coin)
            if word[-num:] == coin:
                real_token = word[:-num]
                break
            else:
                pass
        return real_token
        
    def analyze_title(self, title: str) -> tuple([bool, set]):
        title_lower = title.lower()
        words = self.get_words(title)
        is_delist = False
        delist_token = set()
        #存在delist的关键词才会认为这是下架公告
        for keyword in self.keywords:
            if keyword in title_lower:
                is_delist = True
                break
            else:
                pass
        #检索下架公告中的币种信息，用分词全大写来判断
        if is_delist:
            for word in words:
                if word.isupper() and word not in ["OKX", "OKEX", "BINANCE", "GATE", "GATEIO"]:
                    real_token = self.get_real_token(word)
                    delist_token.add(real_token)
                else:
                    pass
        else:
            pass
        return is_delist, delist_token
    
    def get_delist_coins(self):
        titles = {}
        titles["okex"] = self.get_okex_announcement()
        titles["binance"] = self.get_binance_announcement()
        titles["gate"] = self.get_gate_announcement()
        delist_coins = {"okex": set(), "binance": set(), "gate": set()}
        for exchange in delist_coins.keys():
            for title in titles[exchange]:
                is_delist, delist_token = self.analyze_title(title)
                if is_delist:
                    delist_coins[exchange] = delist_coins[exchange] | delist_token
                    # if len(delist_token) == 0:
                    #     #没有解析出delist的币种
                    #     delist_coins[exchange].add("all")
                    # else:
                    #     pass
                else:
                    pass
        if "OKX" in delist_coins["okex"]:
            delist_coins["okex"].remove("OKX")
        else:
            pass
        self.delist_coins = delist_coins