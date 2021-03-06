#!/usr/local/bin/python3.5
# -*- coding: utf-8 -*-


import auth as au
import ofd as ofd
import validation as vl
import sql as sql
import pandas as pd
import time


def main():
    # подключаемся к ОФД
    response, cooks = au.connect(vl.ofd_idd, vl.ofd_name, vl.ofd_pwd)
    response_y, cooks_y = au.connect(vl.ofd_idd, vl.ofd_name_y, vl.ofd_pwd_y)

    # ==========================
    # KKT
    # получаем список ККТ из ОФД
    print("--------------------------------")
    print("|RECEIVING KKT from OFD")
    start_kkt = time.clock()
    list_kkt = ofd.get_kkt(cooks, inn=vl.inn)
    list_kkt_y = ofd.get_kkt(cooks_y, inn=vl.inn_y)
    print("| TOTAL KKT: ", len(list_kkt)+len(list_kkt_y))
    print("| CPU consumed: ", time.clock() - start_kkt)
    print("--------------------------------\n")
    # записываем ККТ в SQL
    sql.push_kkt(list_kkt)
    sql.push_kkt(list_kkt_y, push=False)
    # ===========================

    # ===========================
    # ФН
    # подготавливаем данные, определяем точный набор полей для корректного залития
    print("--------------------------------")
    print("|READING FN from OFD")
    start_fn = time.clock()
    kkt_list = pd.DataFrame(list_kkt)[['regId']]
    kkt_list_y = pd.DataFrame(list_kkt_y)[['regId']]

    # считываем данные ФН из ОФД
    fn_list = pd.DataFrame()
    for reg_id in kkt_list['regId']:
        fiscal_storage = pd.DataFrame(ofd.get_fn(cooks, reg_id, inn=vl.inn))
        fiscal_storage['regId'] = reg_id
        fn_list = pd.concat([fn_list, fiscal_storage])
        print(fiscal_storage['regId'])
    fn_list_y = pd.DataFrame()
    for reg_id in kkt_list_y['regId']:
        fiscal_storage = pd.DataFrame(ofd.get_fn(cooks_y, reg_id, inn=vl.inn_y))
        fiscal_storage['regId'] = reg_id
        fn_list_y = pd.concat([fn_list_y, fiscal_storage])
        print(fiscal_storage['regId'])
    print("| TOTAL FN: ", len(fn_list)+len(fn_list_y))
    print("| CPU consumed: ", time.clock() - start_fn)
    print("--------------------------------\n")

    # записываем данные ФН в SQL
    sql.push_fn(fn_list)
    sql.push_fn(fn_list_y, push=False)


if __name__ == "__main__":
    main()
