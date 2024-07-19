import os
import json
import base64
import sqlite3
from win32crypt import CryptUnprotectData
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from pycookiecheat import chrome_cookies
import browser_cookie3
import urllib.request
import http.cookiejar
import requests
from requests.cookies import RequestsCookieJar


# chrome://version/
# Shift + Windows
# ./chrome.exe --disable-features=LockProfileCookieDatabase
class BrowserCookie:

    def __get_string(self, local_state):
        with open(local_state, "r", encoding="utf-8") as f:
            s = json.load(f)["os_crypt"]["encrypted_key"]
        return s

    def __pull_the_key(self, base64_encrypted_key):
        encrypted_key_with_header = base64.b64decode(base64_encrypted_key)
        encrypted_key = encrypted_key_with_header[5:]
        key = CryptUnprotectData(encrypted_key, None, None, None, 0)[1]
        return key

    def __decrypt_string(self, key, data):
        nonce, cipherbytes = data[3:15], data[15:]
        aesgcm = AESGCM(key)
        plainbytes = aesgcm.decrypt(nonce, cipherbytes, None)
        plaintext = plainbytes.decode("utf-8")
        return plaintext

    def getCookieFromPath(self, host, type="chrome"):
        if type == "chrome":
            return self.getCookieFromChrome(host)
        elif type == "firefox":
            return self.getCookieFromFirefox(host)
        elif type == "edge":
            return self.getCookieFromEdge(host)

        return None

    def getCookieFromChrome(self, host):
        # local_state = (
        #     os.environ["LOCALAPPDATA"] + r"\Google\Chrome\User Data\Local State"
        # )
        # cookie_path = (
        #     os.environ["LOCALAPPDATA"] + r"\Google\Chrome\User Data\Default\Cookies"
        # )
        # if not os.path.exists(cookie_path):
        #     cookie_path = (
        #         os.environ["LOCALAPPDATA"]
        #         + r"\Google\Chrome\User Data\Default\Network\Cookies"
        #     )

        # Permission denied 需关闭浏览器用
        cookies_jar = browser_cookie3.chrome(
            # cookie_file=cookie_path,
            domain_name=host,
        )
        cookies = {}
        for cookie in cookies_jar:
            cookies[cookie.name] = cookie.value
        # print(cookies)
        return cookies

    def getCookieFromFirefox(self, host):
        return
        local_state = (
            os.environ["LOCALAPPDATA"] + r"\Google\Chrome\User Data\Local State"
        )
        cookie_path = (
            os.environ["LOCALAPPDATA"] + r"\Google\Chrome\User Data\Default\Cookies"
        )
        if not os.path.exists(cookie_path):
            cookie_path = (
                os.environ["LOCALAPPDATA"]
                + r"\Google\Chrome\User Data\Default\Network\Cookies"
            )

        cookies = {}

        #
        cookies_jar = browser_cookie3.firefox(
            # cookie_file=cookie_path,
            domain_name=host,
        )
        for cookie in cookies_jar:
            cookies[cookie.name] = cookie.value

        print(cookies)
        return cookies

    def getCookieFromEdge(self, host):
        local_state = (
            os.environ["LOCALAPPDATA"] + r"\Microsoft\Edge\User Data\Local State"
        )
        cookie_path = (
            os.environ["LOCALAPPDATA"] + r"\Microsoft\Edge\User Data\Default\Cookies"
        )
        if not os.path.exists(cookie_path):
            cookie_path = (
                os.environ["LOCALAPPDATA"]
                + r"\Microsoft\Edge\User Data\Default\Network\Cookies"
            )

        cookies = {}

        #
        cookies_jar = browser_cookie3.edge(
            cookie_file=cookie_path,
            domain_name=host,
        )
        for cookie in cookies_jar:
            cookies[cookie.name] = cookie.value

        print(cookies)
        return cookies

    #
    def getCookieFromUrl(self, url):
        cookie_jar = http.cookiejar.CookieJar()
        session = requests.Session()
        session.cookies = cookie_jar
        response = session.get(url)
        for cookie in cookie_jar:
            print(cookie.name, cookie.value)
        return cookie_jar
