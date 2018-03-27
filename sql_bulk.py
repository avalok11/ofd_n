#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import validation as vl
import pymssql


def push_kkt(kkt_list):
    # ===
    # подготавливаем данные, определяем точный набор полей для корректного залития
    kkt_list = pd.DataFrame(kkt_list)
    kkt_list = kkt_list[['regId', 'model', 'factoryId', 'address', 'status', 'kpp', 'organizationName',
                         'fsFinishDate', 'licenseStartDate', 'licenseFinishDate']]
    # ===
    # добавляем в массиве данных ККТ индексы РЕГид и ФАКТОРИид
    kkt_list = [((x[0], x[2], ) + tuple(x)) for x in kkt_list.values.tolist()]

    # ===========================
    # ОБНОВЛЕНИЕ ДАННЫХ В БАЗЕ
    # ===========================
    # УДАЛЯЕМ ВСЕ И ПОТОМ ВСТАВЛЯЕМ
    conn_ms = pymssql.connect(host=vl.ip_mssql, user=vl.usr_ms, password=vl.pwd_ms,
                              database=vl.db_ms, charset='utf8')

    cursor_ms = conn_ms.cursor()
    cursor_ms.execute('TRUNCATE TABLE RU_T_FISCAL_KKT;')
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

