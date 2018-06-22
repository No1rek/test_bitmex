import time, json, requests
import sqlite3
import threading
from bitmex_websocket import BitMEXWebsocket


class BitMEX:
    def __init__(self, pairs, callback_tg=None, callback_db=None, history_length_s=35, ps_change_callback=0.5):
        self.pairs = pairs
        self.ps_change_callback = ps_change_callback
        self.history_length_s = history_length_s
        self.prices = {}
        self.callback_tg = callback_tg
        self.callback_db = callback_db

        for pair in pairs:
            ws = BitMEXWebsocket(endpoint="https://testnet.bitmex.com/api/v1", symbol=pair, api_key=None,
                                 api_secret=None)
            self.prices[pair] = [ws, []]

    def update_ticker(self):
        message = ""
        if len(self.prices.keys()):
            for k, v in self.prices.items():
                v[1] = [p for p in v[1] if time.time() - p['time'] <= self.history_length_s]
                v[1].append({"time": time.time(), "price": v[0].data['instrument'][0]["midPrice"]})

                max_price = max([p['price'] for p in v[1]])
                percent_change = float(max_price / v[0].data['instrument'][0]["midPrice"] - 1) * 100
                print(k, "is currently ", v[0].data['instrument'][0]["midPrice"], " down change is: ", percent_change, "%")

                if percent_change >= self.ps_change_callback:
                    message += "Цена на рынке {0} упала  на {1}% и теперь составляет {2}\n".format(
                        k, round(percent_change, 2), v[0].data['instrument'][0]["midPrice"])

                    self.callback_db(time.time(), k, v[0].data['instrument'][0]["midPrice"], percent_change)
                    v[1] = [{"time": time.time(), "price": v[0].data['instrument'][0]["midPrice"]}]
            if message:
                self.callback_tg(message)
                print(message)


class Db:
    def __init__(self, name="db.db"):
        self.conn = sqlite3.connect(name)
        self.cursor = self.conn.cursor()

        self.check_db()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def check_db(self):
        tables = self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
        tables = [t[0] for t in tables] if len(tables) > 0 else tables

        if (not "history" in tables):
            self.cursor.execute(
                "CREATE TABLE history (timestamp INT, pair VARCHAR, price REAL, percent_change REAL);")
        if (not "chats" in tables):
            self.cursor.execute("CREATE TABLE chats (chat_id INT NOT NULL);")
            self.conn.commit()

    def select_active_chats(self):
        active_chats = self.cursor.execute("SELECT chat_id FROM chats;").fetchall()
        active_chats = [a[0] for a in active_chats] if len(active_chats) > 0 else active_chats
        return active_chats

    def append_active_chats(self, update):
        self.cursor.execute("INSERT INTO chats VALUES (%s);"
                            % (str(update['message']["chat"]["id"])))
        self.conn.commit()

    def write_event(self, time, pair, price, change):
        self.cursor.execute("INSERT INTO history VALUES (%i, '%s', %f, %f);"
                            % (int(time), pair, round(price, 2), round(change, 2)))
        self.conn.commit()


class Tg:
    def __init__(self):
        self.api_url = "https://api.telegram.org/bot556825305:AAEHg7B4FGF2Rzam7e5tl57EDe7lUjphOdA/"
        self.offset = 0
        self.timeout = 1
        self.active_chats = []
        self.updates = []

        self._updating = threading.Thread(target=self.get_updates)
        self._updating.start()

    def send_message(self, text):
        try:
            thread = threading.Thread(target=self._send_message, args=[text])
            thread.start()
        except Exception as e:
            print(e)

    def get_updates(self):
        while True:
            response = json.loads(requests.get(self.api_url + 'getUpdates', params={"offset": self.offset, "allowed_updates":["message"]}).text)
            self.updates = response['result']
            if len(self.updates) > 0:
                last_update_id = self.updates[-1]['update_id']
                self.offset = last_update_id
            time.sleep(timeout)

    def _send_message(self, text, chat=None):
        if not chat:
            chat = self.active_chats
        if not (isinstance(chat, type([]))):
            chat = [chat]

        for ch in chat:
            params = {'chat_id': ch, 'text': text}
            requests.post(self.api_url + 'sendMessage', data=params)


def load_config():
    with open('config.json') as f:
        data = json.load(f)
    return data


if __name__ == "__main__":
    cfg = load_config()
    timeout = cfg['timeout']

    tg = Tg()
    db = Db()
    tg.active_chats = db.select_active_chats()

    bm = BitMEX(cfg['pairs'], callback_tg=tg.send_message, callback_db=db.write_event)

    while True:
        if len(tg.updates) > 0:
            for update in tg.updates:
                if not update['message']["chat"]["id"] in tg.active_chats :
                    tg.active_chats.append(update['message']["chat"]["id"])
                    tg._send_message("Теперь вы будете получать оповещения об изменении цены", update['message']["chat"]["id"])
                    db.append_active_chats(update)
                    print("Incoming message: ",update['message']['text'])

        bm.update_ticker()

        time.sleep(timeout)
