#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import auth as au
import get_kkt as kkt
import validation as vl
#import sql_bulk as sql
import get_fn as fn
import pandas as pd


def main():
    cooks = au.connect(vl.ofd_idd, vl.ofd_name, vl.ofd_pwd)
    print("\'cooks\':", cooks)

    list_kkt = kkt.get_kkt(cooks, inn='7825335145')
    print(list_kkt)
    print(" TYPE of KKT: ", type(list_kkt))

    # ===
    # подготавливаем данные, определяем точный набор полей для корректного залития
    kkt_list = pd.DataFrame(list_kkt)
    kkt_list = kkt_list[['regId', 'model', 'factoryId', 'address', 'status', 'kpp', 'organizationName',
                         'fsFinishDate', 'licenseStartDate', 'licenseFinishDate']]

    fn_list = pd.DataFrame()
    for reg_id in kkt_list['regId']:
        # print(reg_id)
        fiscal_storage = pd.DataFrame(fn.get_fn(cooks, reg_id, inn='7825335145'))
        # print(fiscal_storage)
        # if 'effectiveTo' not in fiscal_storage.columns.values:
        #    fiscal_storage['effectiveTo'] = None
        # if 'effectiveFrom' not in fiscal_storage.columns.values:
        #    fiscal_storage['effectiveFrom'] = None
        fiscal_storage['regId'] = reg_id
        fn_list = pd.concat([fn_list, fiscal_storage])

    fn_list = fn_list[['regId', 'storageId', 'model', 'status', 'effectiveFrom', 'effectiveTo', 'fsFinishDate']]
    fn_list.fillna('None', inplace=True)
    None
    #sql.push_kkt(list_kkt)


if __name__ == "__main__":
    main()
