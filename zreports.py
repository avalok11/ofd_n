#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import auth as au
import get_kkt as kkt
import validation as vl
import sql_bulk as sql
import get_fn as fn
import pandas as pd
import time
import get_zrep as z
import datetime


def main():
    # определяем даты за которые собираем данные
    date_from = datetime.datetime.strftime((datetime.datetime.today() - datetime.timedelta(days=3)).
                                           replace(microsecond=0).replace(second=0).replace(hour=0).replace(minute=0),
                                           '%Y-%m-%dT%H:%M:%S')
    print(date_from)
    date_to = datetime.datetime.strftime(datetime.datetime.today()
                                         .replace(microsecond=0).replace(second=0).replace(hour=0).replace(minute=0),
                                         '%Y-%m-%dT%H:%M:%S')
    print(date_to)
    # --------------------------
    # ШАГ 1 Получаем список Фискальных накопителей из базы данных
    #fn_list = sql.get_fn()

    # --------------------------
    # ШАГ 2 подключаемся к ОФД
    cooks = au.connect(idd=vl.ofd_idd, login=vl.ofd_name, pwd=vl.ofd_pwd)

    # --------------------------
    # Получаем список Z отчетов из ОФД
    print("--------------------------------")
    print("|RECEIVING Z reports from OFD")
    start = time.clock()
    z_reports_data = pd.DataFrame()
    fiscal_broken = pd.DataFrame()
    count_printers = 0
    i = 0
    fn_list = ('0000583024034213',)
    for fn in fn_list:
        z_rep = pd.DataFrame(z.get_z_rep(cooks, fn[0], date_from, date_to, inn='7825335145'))
        if len(z_rep) == 0:
            fiscal_broken = pd.concat([fiscal_broken, fn[0]])
            count_printers -= 1
        z_reports_data = pd.concat([z_reports_data, z_rep])
        count_printers += 1
        # ДОБАВИТЬ подсчет принтеров с которых мы получили ответ о наличии фискльных смен
        # собрать массив принтеров от которых не было Z отчетов и написать об этом письмо с разбивкой на Дистрикт Ресторан и конкретных ФР
        i += 1
        if i == 9:
            break
    z_reports_data.dropna(axis=0, how='any', inplace=True)
    print("| TOTAL Z REPORTS: ", len(z_reports_data))
    print("| TOTAL FN with Z REPORTS: ", count_printers)
    print("| TOTAL BROKEN FN: ", len(fiscal_broken))
    print("| time consumed: ", time.clock())
    print("--------------------------------\n")

    # --------------------------
    # ШАГ 3 Записываем Z отчеты в SQL
    sql.push_z_reports(z_reports_data)


if __name__ == "__main__":
    main()
