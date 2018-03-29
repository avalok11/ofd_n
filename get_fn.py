#!/usr/local/bin/python3
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
