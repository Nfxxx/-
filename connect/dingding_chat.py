import requests
import json, yaml, os
import logging

logger = logging.getLogger(__name__)


class DingDingChatApi:
    __instance__ = None

    def __new__(cls, *args, **kwargs):
        if not cls.__instance__:
            cls.__instance__ = super(DingDingChatApi, cls).__new__(cls)
        return cls.__instance__

    def __init__(self):
        if not hasattr(self, 'dingding_config'):
            self.dingding_config = self.load_key()
            self.app_key = self.dingding_config['app_key']  # 管理员账号登录开发者平台，应用开发-创建应用-查看详情-appkey
            self.app_secret = self.dingding_config['app_secret']  # 应用里的appsecret
            self.chat_ids = self.dingding_config['chat_ids']
    
    def load_key(self):
        user_path = os.path.expanduser('~')
        cfg_path = os.path.join(user_path, '.dingding')
        if not os.path.exists(cfg_path):
            os.mkdir(cfg_path)
        with open(os.path.join(cfg_path, 'key.yml')) as f:
            ret = yaml.load(f, Loader = yaml.SafeLoader)[0]
            return ret
    
    def get_access_token(self):
        url = f'https://oapi.dingtalk.com/gettoken?appkey={self.app_key}&appsecret={self.app_secret}'
        headers = {
            'Content-Type': "application/x-www-form-urlencoded"
        }
        data = {'appkey': self.app_key, 'appsecret': self.app_secret}
        r = requests.request('GET', url, data=data, headers=headers)
        access_token = r.json()["access_token"]
        return access_token

    def get_media_id(self, file_path):
        access_token = self.get_access_token()  # 拿到接口凭证
        url = 'https://oapi.dingtalk.com/media/upload?access_token=' + access_token + '&type=file'
        files = {'media': open(file_path, 'rb')}
        data = {'access_token': access_token,
                'type': 'file'}
        response = requests.post(url, files=files, data=data)
        json = response.json()
        return json["media_id"]

    def send_file(self, file_path='data_base.xlsx', robot_type='ssf'):
        access_token = self.get_access_token()
        media_id = self.get_media_id(file_path)
        url = 'https://oapi.dingtalk.com/chat/send?access_token=' + access_token
        header = {
            'Content-Type': 'application/json'
        }
        data = {
            'access_token': access_token,
            'chatid': self.chat_ids[robot_type],
            'msg': {
                'msgtype': 'file',
                'file': {'media_id': media_id}
            }
        }
        r = requests.request('POST', url, data=json.dumps(data), headers=header)
        if 200 == r.status_code:
            logger.info(f"ding file send success: {r.text}")
        else:
            logger.error(f'ding file send error : {r.text}')

    def send_message(self, message, robot_type='ssf'):
        access_token = self.get_access_token()
        url = 'https://oapi.dingtalk.com/chat/send?access_token=' + access_token
        header = {
            'Content-Type': 'application/json'
        }
        data = {
            'access_token': access_token,
            'chatid': self.chat_ids[robot_type],
            'msg': {
                "msgtype": "text",
                "text": {
                    "content": message
                }
            }
        }
        r = requests.request('POST', url, data=json.dumps(data), headers=header)
        if 200 == r.status_code:
            logger.info(f"ding file send success: {r.text}")
        else:
            logger.error(f'ding file send error : {r.text}')



