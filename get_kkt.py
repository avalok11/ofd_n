#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import requests


def get_kkt(cooks, inn='7825335145', status=2):
    """
    Список ККТ по организации
    :param cooks: получаем данные после авторизации на сайте connect()
    :param inn: ИНН организации
    :param status: Статус регистрации ККТ в ОФД:
                    0 – не зарегистрирована,
                    1 – идёт регистрация,
                    2 – активирована,
                    3 – снята с регистрации,
                    4 – ожидание активации.
    :return:
                    regId	    String, обязательный	Регистрационный номер ККТ, выданный ФНС
                    model	    String, обязательный	Название модели ККТ
                    factoryId	String, обязательный	Заводской номер ККТ
                    address	    String, обязательный	Адрес установки ККТ
                    status	    Number, обязательный	Статус регистрации ККТ в ОФД.
    """
    response = requests.get('https://api.sbis.ru/ofd/v1/orgs/' + str(inn) + '/kkts',
                            cookies=cooks)
    print(response)
    return response.json()
