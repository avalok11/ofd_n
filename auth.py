#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import requests
import validation as vl
import json


def connect(idd=vl.ofd_idd, login=vl.ofd_name, pwd=vl.ofd_pwd):
    """
    Аутентификация
    :param idd: application organisation id - from SBIS
    :param login: login name
    :param pwd: password
    :return: COOKS - cookies which is used in next GET requests
            sid     String, обязательный        Идентификатор сессии
            token       String, необязательный  Не используется
    """
    payload = {'app_client_id': idd, 'login': login, 'password': pwd}
    response = requests.post('https://api.sbis.ru/oauth/service/',
                             json=payload)
    # data=json.dumps(payload),
    # headers={'content-type': 'application/json; charset=utf-8'})
    print("\n--------------------------------")
    print("CONNECTION IS ESTABLISHED")
    print("  STATUS: ", response.status_code)
    print("  URL: ", response.url)
    print("  CONNECT: ", response.content)
    # response.raise_for_status()
    cooks = json.loads(response.content.decode('utf-8'))['sid']
    token = json.loads(response.content.decode('utf-8'))['token']
    print("  COOKS: ", cooks)
    print("  OKEN: ", token)
    print("--------------------------------")
    print("\n")
    return cooks, token
