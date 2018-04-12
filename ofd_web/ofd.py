#!/usr/local/bin/python3.5
# -*- coding: utf-8 -*-

import requests


def get_fn(cooks, reg_id, inn='7825335145', status=2):
    """
    Список фискальных накопителей по ККТ
    :param cooks: получаем данные после авторизации на сайте connect()
    :param inn: ИНН организации
    :param status: Статус регистрации ККТ в ОФД:
                    0 – не зарегистрирована,
                    1 – идёт регистрация,
                    2 – активирована,
                    3 – снята с регистрации,
                    4 – ожидание активации.
    :param reg_id: Регистрационный номер ККТ, выданный ФНС, получаем из базы данных RU_T_FISCAL_KKT
    :return:
    storageId	String, обязательный	Номер фискального накопителя	«9999999»
    model	String, обязательный	Название модели ККТ	«ФН-Модель»
    status	Number, обязательный	Статус регистрации ФН в ОФД.	«2»
    effectiveFrom	String, обязательный	Время начала работы накопителя	«2016-10-19T12:20:45»
    effectiveTo	String	Время окончания работы накопителя, отсутствует для действующего накопителя	«2016-11-19T23:20:45»
    """
    response = requests.get('https://api.sbis.ru/ofd/v1/orgs/' + str(inn) + '/kkts/' + str(reg_id) + '/storages',
                            cookies=cooks)
    return response.json()


def get_z_rep(cooks, reg_id, date_from, date_to, inn='7825335145'):
    """


    """
    response = requests.get('https://api.sbis.ru/ofd/v1/orgs/' + str(inn) + '/kkts/' + str(reg_id) + '/shifts?dateFrom='
                            + str(date_from) + '&dateTo=' + str(date_to) + '&accounting=byOpen&limit=1000',
                            cookies=cooks)
    return response.json()


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
    return response.json()

