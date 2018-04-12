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
    cooks = au.connect(vl.ofd_idd, vl.ofd_name, vl.ofd_pwd)

    # ==========================
    # KKT
    # получаем список ККТ из ОФД
    print("--------------------------------")
    print("|RECEIVING KKT from OFD")
    start_kkt = time.clock()
    list_kkt = ofd.get_kkt(cooks, inn='7825335145')
    print("| TOTAL KKT: ", len(list_kkt))
    print("| CPU consumed: ", time.clock()-start_kkt)
    print("--------------------------------\n")
    # записываем ККТ в SQL
    sql.push_kkt(list_kkt)
    # ===========================

    # ===========================
    # ФН
    # подготавливаем данные, определяем точный набор полей для корректного залития
    print("--------------------------------")
    print("|READING FN from OFD")
    start_fn = time.clock()
    kkt_list = pd.DataFrame(list_kkt)[['regId']]

    # считываем данные ФН из ОФД
    fn_list = pd.DataFrame()
    for reg_id in kkt_list['regId']:
        fiscal_storage = pd.DataFrame(ofd.get_fn(cooks, reg_id, inn='7825335145'))
        fiscal_storage['regId'] = reg_id
        fn_list = pd.concat([fn_list, fiscal_storage])
    print("| TOTAL FN: ", len(fn_list))
    print("| CPU consumed: ", time.clock()-start_fn)
    print("--------------------------------\n")

    # записываем данные ФН в SQL
    sql.push_fn(fn_list)


if __name__ == "__main__":
    main()
