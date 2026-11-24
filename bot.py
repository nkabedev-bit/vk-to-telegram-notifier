#!/usr/bin/env python3
"""Пересылает входящие сообщения сообществ ВКонтакте в Telegram.

Бот только уведомляет: в ВК он ничего не отвечает, сообщения прочитанными не помечает
и настройки сообществ не меняет. Токенам сообществ достаточно права «Сообщения сообщества».

Запуск:            python3 bot.py
Проверка настроек: python3 bot.py --check
"""

import http.client
import json
import logging
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

CONFIG_PATH = Path(__file__).with_name("config.json")
REQUIRED_CONFIG_KEYS = ("telegram_bot_token", "telegram_chat_id", "vk_community_tokens")

VK_API_URL = "https://api.vk.com/method/"
VK_API_VERSION = "5.199"
VK_AUTH_FAILED_CODES = {5, 27}  # токен недействителен / отозван
GROUP_CHAT_PEER_OFFSET = 2_000_000_000  # peer_id бесед начинаются с этого числа

# Long Poll сообщений, версия 3: https://dev.vk.com/ru/api/user-long-poll/getting-started
LONG_POLL_VERSION = 3
LONG_POLL_WAIT_SECONDS = 25
EVENT_NEW_MESSAGE = 4
FLAG_OUTBOX = 2  # сообщение отправлено самим сообществом

TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/{method}"
TELEGRAM_MESSAGE_LIMIT = 4096

HTTP_TIMEOUT_SECONDS = LONG_POLL_WAIT_SECONDS + 10
CONNECT_TIMEOUT_SECONDS = 10  # TCP + TLS: зависшее рукопожатие бросаем быстро, а не через 35 с
RETRY_DELAY_SECONDS = 5

NETWORK_ERRORS = (OSError, http.client.HTTPException, json.JSONDecodeError)

log = logging.getLogger("vk2tg")


class VkApiError(Exception):
    def __init__(self, code, message):
        super().__init__(f"ошибка VK API {code}: {message}")
        self.code = code


def http_json(url, *, query=None, form=None, timeout=HTTP_TIMEOUT_SECONDS):
    if query:
        url = f"{url}?{urllib.parse.urlencode(query)}"
    data = urllib.parse.urlencode(form).encode() if form is not None else None
    with urllib.request.urlopen(url, data=data, timeout=timeout) as response:
        return json.load(response)


class KeepAliveHttp:
    """HTTPS-клиент, который держит соединения открытыми между запросами.

    Каждое новое соединение — это TLS-рукопожатие, а на маршруте хостинг → ВК они периодически
    зависают. Переиспользование соединений сокращает число рукопожатий в десятки раз.
    Не потокобезопасен: у каждого потока свой экземпляр. Интерфейс как у http_json.
    """

    def __init__(self):
        self._connections = {}

    def request_json(self, url, *, query=None, form=None, timeout=HTTP_TIMEOUT_SECONDS):
        parts = urllib.parse.urlsplit(url)
        target = parts.path or "/"
        if query:
            target += f"?{urllib.parse.urlencode(query)}"
        body = urllib.parse.urlencode(form).encode() if form is not None else None
        try:
            try:
                return self._send(parts.netloc, target, body, timeout)
            except (ConnectionResetError, BrokenPipeError):
                # Сервер мог закрыть простаивавшее соединение — один раз повторяем на свежем.
                return self._send(parts.netloc, target, body, timeout)
        except NETWORK_ERRORS as error:
            raise ConnectionError(f"{parts.netloc}: {error}") from error

    def _send(self, host, target, body, timeout):
        connection = self._connections.get(host)
        if connection is None:
            connection = self._connections[host] = http.client.HTTPSConnection(
                host, timeout=CONNECT_TIMEOUT_SECONDS)
        try:
            if connection.sock is None:
                connection.connect()  # рукопожатие идёт под коротким таймаутом
            connection.sock.settimeout(timeout)
            if body is None:
                connection.request("GET", target)
            else:
                connection.request("POST", target, body=body,
                                   headers={"Content-Type": "application/x-www-form-urlencoded"})
            response = connection.getresponse()
            payload = response.read()
        except BaseException:
            connection.close()  # следующее обращение откроет соединение заново
            raise
        if response.status != 200:
            raise http.client.HTTPException(f"HTTP {response.status}")
        return json.loads(payload)


def vk_call(method, token, request_json=http_json, **params):
    payload = request_json(VK_API_URL + method, form={**params, "access_token": token, "v": VK_API_VERSION})
    if "error" in payload:
        error = payload["error"]
        raise VkApiError(error.get("error_code"), error.get("error_msg"))
    return payload["response"]


def fetch_group(token, request_json=http_json):
    response = vk_call("groups.getById", token, request_json)
    groups = response["groups"] if isinstance(response, dict) else response
    return {"id": groups[0]["id"], "name": groups[0]["name"]}


def get_long_poll_server(token, group_id, request_json=http_json):
    return vk_call("messages.getLongPollServer", token, request_json,
                   group_id=group_id, lp_version=LONG_POLL_VERSION)


def dialog_link(group_id, peer_id):
    selection = f"c{peer_id - GROUP_CHAT_PEER_OFFSET}" if peer_id > GROUP_CHAT_PEER_OFFSET else peer_id
    return f"https://vk.com/gim{group_id}?sel={selection}"


def format_notification(group, sender_name, message):
    return (
        f"Название сообщества: {group['name']}\n\n"
        f"Имя Фамилия: {sender_name}\n\n"
        f"Текст сообщения: {message.get('text', '')}\n\n"
        f"Ссылка на диалог: {dialog_link(group['id'], message['peer_id'])}"
    )


def telegram_error_details(error):
    try:
        body = json.loads(error.read())
    except (ValueError, OSError):
        return str(error), None
    return body.get("description", str(error)), body.get("parameters", {}).get("retry_after")


class TelegramNotifier:
    def __init__(self, token, chat_id):
        self._send_url = TELEGRAM_API_URL.format(token=token, method="sendMessage")
        self._chat_id = chat_id
        self._lock = threading.Lock()  # шлём по одному, чтобы не упереться в лимиты Telegram

    def send(self, text):
        with self._lock:
            for start in range(0, len(text), TELEGRAM_MESSAGE_LIMIT):
                self._send_chunk(text[start:start + TELEGRAM_MESSAGE_LIMIT])

    def _send_chunk(self, text):
        form = {
            "chat_id": self._chat_id,
            "text": text,
            "link_preview_options": json.dumps({"is_disabled": True}),
        }
        while True:
            try:
                http_json(self._send_url, form=form)
                return
            except urllib.error.HTTPError as error:
                description, retry_after = telegram_error_details(error)
                if error.code == 429:
                    time.sleep(retry_after or RETRY_DELAY_SECONDS)
                    continue
                if error.code < 500:
                    log.error(
                        "Telegram не принял сообщение (%s: %s). Если «chat not found» или «bot was blocked» — "
                        "получатель должен открыть бота и нажать «Старт».",
                        error.code, description,
                    )
                    return
                log.warning("Telegram ответил %s: %s, повторяю", error.code, description)
            except NETWORK_ERRORS as error:
                log.warning("Telegram недоступен (%s), повторяю", error)
            time.sleep(RETRY_DELAY_SECONDS)


class CommunityListener(threading.Thread):
    def __init__(self, token, notifier, label):
        super().__init__(name=label, daemon=True)
        self._token = token
        self._notifier = notifier
        self._group = None
        self._ts = None  # переживает переподключения, чтобы не терять сообщения при сбоях сети
        self._request_json = KeepAliveHttp().request_json

    def run(self):
        while True:
            try:
                if self._group is None:
                    self._group = fetch_group(self._token, self._request_json)
                    log.info("Слушаю «%s» (id %s)", self._group["name"], self._group["id"])
                self._poll()
            except VkApiError as error:
                if error.code in VK_AUTH_FAILED_CODES:
                    log.error("%s: %s. Токен недействителен — замените его в config.json и перезапустите бота.",
                              self._display_name(), error)
                    return
                log.warning("%s: %s, повторяю", self._display_name(), error)
            except NETWORK_ERRORS as error:
                log.warning("%s: сеть недоступна (%s), повторяю", self._display_name(), error)
            except Exception:
                log.exception("%s: непредвиденная ошибка, повторяю", self._display_name())
            time.sleep(RETRY_DELAY_SECONDS)

    def _display_name(self):
        return f"«{self._group['name']}»" if self._group else self.name

    def _poll(self):
        server = get_long_poll_server(self._token, self._group["id"], self._request_json)
        if self._ts is None:
            self._ts = server["ts"]
        while True:
            response = self._request_json(f"https://{server['server']}", query={
                "act": "a_check", "key": server["key"], "ts": self._ts,
                "wait": LONG_POLL_WAIT_SECONDS, "mode": 0, "version": LONG_POLL_VERSION,
            })
            failed = response.get("failed")
            if failed == 1:  # история событий устарела — берём новый ts
                self._ts = response["ts"]
                continue
            if failed == 2:  # истёк ключ — новый ключ, старый ts, чтобы не потерять события
                server = get_long_poll_server(self._token, self._group["id"], self._request_json)
                continue
            if failed is not None:  # 3 — потеряна информация, 4 — неверная версия: начинаем заново
                log.warning("%s: Long Poll вернул failed=%s, переподключаюсь", self._display_name(), failed)
                server = get_long_poll_server(self._token, self._group["id"], self._request_json)
                self._ts = server["ts"]
                continue
            self._forward_incoming(response.get("updates", []))
            self._ts = response["ts"]  # только после пересылки: при сбое события придут повторно

    def _forward_incoming(self, updates):
        incoming_ids = [
            event[1] for event in updates
            if event[0] == EVENT_NEW_MESSAGE and not event[2] & FLAG_OUTBOX
        ]
        if not incoming_ids:
            return
        # Текст берём через API: в событии Long Poll он искажён HTML-экранированием.
        # extended=1 заодно возвращает профили отправителей — отдельный users.get не нужен.
        response = vk_call("messages.getById", self._token, self._request_json,
                           message_ids=",".join(map(str, incoming_ids)), extended=1)
        names = {
            profile["id"]: f"{profile['first_name']} {profile['last_name']}"
            for profile in response.get("profiles", [])
        }
        for message in response["items"]:
            sender_name = names.get(message["from_id"], "")
            self._notifier.send(format_notification(self._group, sender_name, message))
            log.info("Переслал сообщение из «%s»", self._group["name"])


def load_config():
    """Настройки из переменных окружения (хостинг) или из config.json (локальный запуск)."""
    telegram_token = os.environ.get("TELEGRAM_BOT_TOKEN") or os.environ.get("BOT_TOKEN")
    if telegram_token:
        config = {
            "telegram_bot_token": telegram_token,
            "telegram_chat_id": os.environ.get("TELEGRAM_CHAT_ID", "").strip(),
            "vk_community_tokens": [
                token.strip() for token in os.environ.get("VK_COMMUNITY_TOKENS", "").split(",") if token.strip()
            ],
        }
        source = "переменных окружения (BOT_TOKEN, TELEGRAM_CHAT_ID, VK_COMMUNITY_TOKENS)"
    elif CONFIG_PATH.exists():
        config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        source = CONFIG_PATH.name
    else:
        sys.exit("Нет настроек: задайте переменные окружения BOT_TOKEN, TELEGRAM_CHAT_ID, VK_COMMUNITY_TOKENS "
                 "или создайте config.json по образцу config.example.json.")
    missing = [key for key in REQUIRED_CONFIG_KEYS if not config.get(key)]
    if missing:
        sys.exit(f"В {source} не заполнено: {', '.join(missing)}")
    return config


def run_check(config):
    """Только читает: ничего не отправляет и не меняет."""
    telegram_token = config["telegram_bot_token"]
    chat_id = config["telegram_chat_id"]
    try:
        bot = http_json(TELEGRAM_API_URL.format(token=telegram_token, method="getMe"))["result"]
        print(f"Telegram-бот: @{bot['username']}")
    except urllib.error.HTTPError as error:
        print(f"Telegram-бот: токен не работает ({telegram_error_details(error)[0]})")
    try:
        http_json(TELEGRAM_API_URL.format(token=telegram_token, method="getChat"), query={"chat_id": chat_id})
        print(f"Получатель {chat_id}: бот может ему писать")
    except urllib.error.HTTPError as error:
        print(f"Получатель {chat_id}: бот пока не может ему писать "
              f"({telegram_error_details(error)[0]}) — нужно открыть бота и нажать «Старт»")

    for number, token in enumerate(config["vk_community_tokens"], 1):
        try:
            group = fetch_group(token)
            get_long_poll_server(token, group["id"])
        except VkApiError as error:
            print(f"{number}. не работает: {error}")
            continue
        print(f"{number}. {group['name']} (id {group['id']}) — готово")
    return 0


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config = load_config()
    if "--check" in sys.argv[1:]:
        return run_check(config)

    notifier = TelegramNotifier(config["telegram_bot_token"], config["telegram_chat_id"])
    listeners = [
        CommunityListener(token, notifier, f"Токен №{number}")
        for number, token in enumerate(config["vk_community_tokens"], 1)
    ]
    for listener in listeners:
        listener.start()
    try:
        while any(listener.is_alive() for listener in listeners):
            time.sleep(1)
    except KeyboardInterrupt:
        return 0
    log.critical("Не слушается ни одно сообщество — причины в логе выше.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
