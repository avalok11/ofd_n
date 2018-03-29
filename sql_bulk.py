#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import validation as vl
import pymssql
from datetime import datetime
import time


def push_kkt(kkt_list):
    # ===
    # подготавливаем данные, определяем точный набор полей для корректного залития
    print("--------------------------------")
    print("| SENDING KKT LIST to SQL")
    start = time.clock()
    kkt_list = pd.DataFrame(kkt_list)
    kkt_list = kkt_list[['regId', 'model', 'factoryId', 'address', 'status', 'kpp', 'organizationName',
                         'fsFinishDate', 'licenseStartDate', 'licenseFinishDate']]
    kkt_list['fsFinishDate'].fillna('1980-08-03', inplace=True)
    kkt_list['licenseStartDate'].fillna('1980-08-03', inplace=True)
    kkt_list['licenseFinishDate'].fillna('1980-08-03', inplace=True)
    kkt_list.fillna('None', inplace=True)
    # ===
    # добавляем в массиве данных ККТ индексы РЕГид и ФАКТОРИид
    kkt_list = [((x[0], x[2], ) + tuple(x[:-3]) + (datetime.strptime(x[-3], '%Y-%m-%d'),
                                                   datetime.strptime(x[-2], '%Y-%m-%d'),
                                                   datetime.strptime(x[-1], '%Y-%m-%d')))
                for x in kkt_list.values.tolist()]

    # ===========================
    # ОБНОВЛЕНИЕ ДАННЫХ В БАЗЕ
    # ===========================
    # УДАЛЯЕМ ВСЕ И ПОТОМ ВСТАВЛЯЕМ
    conn_ms = pymssql.connect(host=vl.ip_mssql, user=vl.usr_ms, password=vl.pwd_ms,
                              database=vl.db_ms, charset='utf8')

    cursor_ms = conn_ms.cursor()
    cursor_ms.execute('TRUNCATE TABLE RU_T_FISCAL_KKTn;')
    cursor_ms.executemany("BEGIN "
                          "  IF NOT EXISTS "
                          "    (SELECT 1 FROM RU_T_FISCAL_KKTn WHERE regId=%s AND factoryId=%s )"
                          "  BEGIN "
                          "    INSERT INTO RU_T_FISCAL_KKTn (regId, model, factoryId, address, status, kpp,"
                          "                                 organizationName, fsFinishDate, licenseStartDate,"
                          "                                 licenseFinishDate) "
                          "    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                          "  END "
                          "END", kkt_list)

    conn_ms.commit()
    conn_ms.close()
    print("| DATA SAVED")
    print("| time consumed: ", time.clock())
    print("--------------------------------\n")


def push_fn(fn_list):
    # ===
    # подготавливаем данные, определяем точный набор полей для корректного залития
    print("--------------------------------")
    print("| SENDING FN LIST to SQL")
    start = time.clock()
    fn_list = pd.DataFrame(fn_list)
    if 'effectiveTo' not in fn_list.columns.values:
        fn_list['effectiveTo'] = None
    if 'effectiveFrom' not in fn_list.columns.values:
        fn_list['effectiveFrom'] = None
    if 'fsFinishDate' not in fn_list.columns.values:
        fn_list['fsFinishDate'] = None
    fn_list = fn_list[['regId', 'storageId', 'model', 'status', 'effectiveFrom', 'effectiveTo', 'fsFinishDate']]
    fn_list['effectiveFrom'].fillna('1980-08-03T13:57:00', inplace=True)
    fn_list['effectiveTo'].fillna('1980-08-03T13:57:00', inplace=True)
    fn_list['fsFinishDate'].fillna('1980-08-03', inplace=True)
    fn_list.fillna('None', inplace=True)

    # ===
    # добавляем в массиве данных ФН индексы РЕГид и ФАКТОРИид
    fn_list = [((x[0], x[1], ) + tuple(x[:-3]) + (datetime.strptime(x[-3], '%Y-%m-%dT%H:%M:%S'),
                                                  datetime.strptime(x[-2], '%Y-%m-%dT%H:%M:%S'),
                                                  datetime.strptime(x[-1], '%Y-%m-%d')))
               for x in fn_list.values.tolist()]

    # ===========================
    # ОБНОВЛЕНИЕ ДАННЫХ В БАЗЕ
    # ===========================
    # УДАЛЯЕМ ВСЕ И ПОТОМ ВСТАВЛЯЕМ
    conn_ms = pymssql.connect(host=vl.ip_mssql, user=vl.usr_ms, password=vl.pwd_ms,
                              database=vl.db_ms, charset='utf8')

    cursor_ms = conn_ms.cursor()
    cursor_ms.execute('TRUNCATE TABLE RU_T_FISCAL_FNn;')
    cursor_ms.executemany("BEGIN "
                          "  IF NOT EXISTS "
                          "    (SELECT 1 FROM RU_T_FISCAL_FNn WHERE regId=%s AND storageId=%s )"
                          "  BEGIN "
                          "    INSERT INTO RU_T_FISCAL_FNn (regId, storageId, model, status, effectiveFrom,"
                          "                                  effectiveTo, fsFinishDate) "
                          "    VALUES (%s, %s, %s, %s, %s, %s, %s)"
                          "  END "
                          "END", fn_list)

    conn_ms.commit()
    conn_ms.close()
    print("| DATA SAVED")
    print("| time consumed: ", time.clock())
    print("--------------------------------\n")
