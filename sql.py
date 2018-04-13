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
    start_sql = time.clock()
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
    cursor_ms.execute('DELETE FROM RU_T_FISCAL_KKTn;')
    cursor_ms.executemany("BEGIN "
                          "  IF NOT EXISTS "
                          "    (SELECT 1 FROM RU_T_FISCAL_KKT WHERE regId=%s AND factoryId=%s )"
                          "  BEGIN "
                          "    INSERT INTO RU_T_FISCAL_KKT (regId, model, factoryId, address, status, kpp,"
                          "                                 organizationName, fsFinishDate, licenseStartDate,"
                          "                                 licenseFinishDate) "
                          "    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                          "  END "
                          "END", kkt_list)

    conn_ms.commit()
    conn_ms.close()
    print("| DATA SAVED")
    print("| CPU consumed: ", time.clock()-start_sql)
    print("--------------------------------\n")


def push_fn(fn_list):
    # ===
    # подготавливаем данные, определяем точный набор полей для корректного залития
    print("--------------------------------")
    print("| SENDING FN LIST to SQL")
    start_fn = time.clock()
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
    cursor_ms.execute('DELETE FROM RU_T_FISCAL_FN;')
    cursor_ms.executemany("BEGIN "
                          "  IF NOT EXISTS "
                          "    (SELECT 1 FROM RU_T_FISCAL_FN WHERE regId=%s AND storageId=%s )"
                          "  BEGIN "
                          "    INSERT INTO RU_T_FISCAL_FN (regId, storageId, model, status, effectiveFrom,"
                          "                                  effectiveTo, fsFinishDate) "
                          "    VALUES (%s, %s, %s, %s, %s, %s, %s)"
                          "  END "
                          "END", fn_list)

    conn_ms.commit()
    conn_ms.close()
    print("| DATA SAVED")
    print("| CPU consumed: ", time.clock()-start_fn)
    print("--------------------------------\n")


def get_fn():
    # ==============================
    # подключаемся к SQL
    print("\n--------------------------------")
    print("|CONNECTING to SQL")
    start_fn = time.clock()
    conn_ms = pymssql.connect(host=vl.ip_mssql, user=vl.usr_ms, password=vl.pwd_ms,
                              database=vl.db_ms, charset='utf8')

    cursor_ms = conn_ms.cursor()
    cursor_ms.execute("SELECT regId FROM RU_T_FISCAL_FN WHERE status=2;")
    fn = cursor_ms.fetchall()
    conn_ms.close()

    print("| DATA DOWNLOADED")
    print("| CPU consumed: ", time.clock()-start_fn)
    print("--------------------------------")
    return fn


def push_z_reports(z_reports):
    # ===
    # подготавливаем данные, определяем точный набор полей для корректного залития
    print("--------------------------------")
    print("|SENDING Z REPORTS LIST to SQL")
    start_z = time.clock()
    z_reports = pd.DataFrame(z_reports)
    z_reports['kktNumber2'] = z_reports['kktNumber']
    z_reports['fsNumber2'] = z_reports['fsNumber']
    z_reports['kktRegId2'] = z_reports['kktRegId']
    z_reports['shiftNumber2'] = z_reports['shiftNumber']
    z_reports = z_reports[['kktNumber2', 'fsNumber2', 'kktRegId2', 'shiftNumber2', 'inn', 'kpp', 'organizationName',
                           'shiftNumber', 'dateTimeOpen', 'dateTimeClose',
                           'incomeSum', 'cashSum', 'eCashSum', 'returnCashSum', 'returnECashSum', 'outcomeSum',
                           'nds10', 'nds18', 'incomeCount', 'incomeReturnCount', 'outcomeCount', 'outcomeReturnCount',
                           'receiptCorrectionCountSell', 'receiptCorrectionCountBuy', 'totalSumSellCorrection',
                           'totalSumBuyCorrection', 'kktName', 'kktAddress', 'kktNumber', 'fsNumber', 'kktRegId',
                           'shiftDocNumber', 'kktSalesPoint']]
    z_reports['dateTimeOpen'] = pd.to_datetime(z_reports['dateTimeOpen'], format='%d.%m.%y %H:%M')
    z_reports['dateTimeClose'] = pd.to_datetime(z_reports['dateTimeClose'], format='%d.%m.%y %H:%M')
    # ===
    # переводим datetime в строку
    z_reports = list((tuple(x[:]) for x in z_reports.values.tolist()))
    # ===========================
    # ОБНОВЛЕНИЕ ДАННЫХ В БАЗЕ
    # ===========================
    # УДАЛЯЕМ ВСЕ И ПОТОМ ВСТАВЛЯЕМ
    conn_ms = pymssql.connect(host=vl.ip_mssql, user=vl.usr_ms, password=vl.pwd_ms,
                              database=vl.db_ms, charset='utf8')
    cursor_ms = conn_ms.cursor()
    cursor_ms.executemany("BEGIN "
                          "  IF NOT EXISTS "
                          "    (SELECT 1 FROM RU_T_FISCAL_Z "
                          "                 WHERE kktNumber=%s AND fsNumber=%s AND kktRegId=%s AND shiftNumber=%s)"
                          "  BEGIN "
                          "    INSERT INTO RU_T_FISCAL_Z (inn, kpp, organizationName, shiftNumber, dateTimeOpen, "
                          " dateTimeClose, incomeSum, cashSum, eCashSum, returnCashSum, returnECashSum, outcomeSum, "
                          " nds10, nds18, incomeCount, incomeReturnCount, outcomeCount, outcomeReturnCount, "
                          " receiptCorrectionCountSell, receiptCorrectionCountBuy, totalSumSellCorrection, "
                          " totalSumBuyCorrection, kktName, kktAddress, kktNumber, fsNumber, kktRegId, shiftDocNumber, "
                          " kktSalesPoint) "
                          "    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                          "            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                          "            %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                          "  END "
                          "END", z_reports)

    conn_ms.commit()
    conn_ms.close()
    print("| DATA SAVED")
    print("| CPU consumed: ", time.clock()-start_z)
    print("--------------------------------\n")


def push_broken_fn(fiscal_broken, date_to):
    # ===
    # подготавливаем данные, определяем точный набор полей для корректного залития
    print("--------------------------------")
    print("| SENDING BROKEN FN to SQL")
    start_fn = time.clock()
    fiscal_broken = tuple(fiscal_broken)

    # ===========================
    # ОБНОВЛЕНИЕ ДАННЫХ В БАЗЕ
    # ===========================
    # УДАЛЯЕМ ВСЕ И ПОТОМ ВСТАВЛЯЕМ
    conn_ms = pymssql.connect(host=vl.ip_mssql, user=vl.usr_ms, password=vl.pwd_ms,
                              database=vl.db_ms, charset='utf8')

    cursor_ms = conn_ms.cursor()
    cursor_ms.executemany("DELETE FROM RU_T_FISCAL_BROKEN_FN WHERE regId=%s AND dateTo=%s", fiscal_broken)
    conn_ms.commit()
    cursor_ms.executemany("BEGIN "
                          "    INSERT INTO RU_T_FISCAL_BROKEN_FN (regId, dateTo) "
                          "    VALUES (%s, %s)"
                          "END", fiscal_broken)

    conn_ms.commit()
    conn_ms.close()
    print("| DATA SAVED")
    print("| CPU consumed: ", time.clock()-start_fn)
    print("--------------------------------\n")

