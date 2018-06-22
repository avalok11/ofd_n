#!/usr/local/bin/python3.5
# -*- coding: utf-8 -*-


import auth as au
import validation as vl
import sql as sql
import pandas as pd
import time
import ofd as z
import sys
import cgitb
from datetime import datetime


def main():
    #part of HTML
    cgitb.enable()
    print("Content-Type: text/html;charset=utf-8")
    print()

    # get data from WEB string
    try:
        param = sys.argv[1]
    except IndexError:
        print("ERROR! <br><br>Please enter KKTnumber, start date and end date <br>")
        sys.exit("Failed to read kktnumber and dates")
    param = param.split()
    print("Enter parameters:<br>", param, "<br>")
    if len(param) != 4:
        print("<br>ERROR wrong data <br>")
        print("<br>Failed number of parameters.<br>The amount of parameters total=4.<br>Example: http://192.168.57.227/ofd/z_reports.py 0001604042030746 2018-04-04T00:00:00 2018-04-05T00:00:00 1")
        sys.exit("Failed number of parameters. The amount of parameters total=4. Example: http://192.168.57.227/ofd/z_reports.py?0001604042030746%202018-04-04T00:00:00%202018-04-05T00:00:00%201. Where %20 - SPACE.")

    try:
        dateF = datetime.strptime(param[1],'%Y-%m-%dT%H:%M:%S')
        dateT = datetime.strptime(param[2],'%Y-%m-%dT%H:%M:%S')
    except ValueError:
        print("<br>ERROR datetime type <br>")
        print("Datetime should be: %Y-%m-%dT%H:%M:%S. <br>Example: 2018-04-04T00:00:00")
        sys.exit("Error datetime format.")
    if dateF>dateT:
        print("<br>ERROR datetime start stop <br>")
        print("First datetime should be less then last datetime.<br>Example: http://192.168.57.227/ofd/z_reports.py 0001604042030746 2018-04-04T00:00:00 2018-04-05T00:00:00 1")
        sys.exit("Error datetime start stop.")

    fn_list = [(param[0],)]
    date_from = param[1]
    date_to = param[2]
    amrest = param[3]
    amrest = int(amrest)


    # logs
    old_stdout = sys.stdout
    today = datetime.today().replace(microsecond=0).replace(second=0).replace(hour=0).replace(minute=0)
    day = datetime.today().date().isoformat()
    hour = datetime.today().hour
    minute = datetime.today().minute
    filename = "/usr/local/www/ofd/logs/AmRest_message_"+day+"_"+str(hour)+"_"+str(minute)+".log"
    log_file = open(filename, "w")
    sys.stdout = log_file
    print("Start logs.")

    print(date_from)
    print(date_to)

    # --------------------------
    # ШАГ 1 Получаем список Фискальных накопителей из базы данных
    # fn_list = sql.get_fn()
    #fn_list = [('0000546299024021',)]

    # --------------------------
    # ШАГ 2 подключаемся к ОФД
    if amrest==1:
        cooks = au.connect(idd=vl.ofd_idd, login=vl.ofd_name, pwd=vl.ofd_pwd)
    elif amrest==2:
        cooks = au.connect(idd=vl.ofd_idd, login=vl.ofd_name_y, pwd=vl.ofd_pwd_y)
    print ("Cooks:<br>", cooks, "<br>")

    # --------------------------
    # Получаем список Z отчетов из ОФД
    print("--------------------------------")
    print("|RECEIVING Z reports from OFD")
    start_z = time.clock()
    z_reports_data = pd.DataFrame()
    fiscal_broken = list()
    count_printers = 0
    i = 0
    # fn_list = ('0000583024034213',)
    for fn in fn_list:
        if amrest==1:
            z_rep = pd.DataFrame(z.get_z_rep(cooks, fn[0], date_from, date_to, inn=vl.inn))
        elif amrest==2:
            z_rep = pd.DataFrame(z.get_z_rep(cooks, fn[0], date_from, date_to, inn=vl.inn_y))
        #else:
        #    int z_rep
        if len(z_rep) == 0:
            fiscal_broken.append((fn[0], today))
            count_printers -= 1
        z_reports_data = pd.concat([z_reports_data, z_rep])
        count_printers += 1
        # для теста
        # i += 1
        # if i == 19:
        #     break
    z_reports_data.dropna(axis=0, how='any', inplace=True)
    print("| TOTAL Z REPORTS: ", len(z_reports_data))
    print("| TOTAL FN with Z REPORTS: ", count_printers)
    print("| TOTAL BROKEN FN: ", len(fiscal_broken))
    print("| CPU consumed: ", time.clock()-start_z)
    print("--------------------------------\n")

    # --------------------------
    # ШАГ 3 Записываем Z отчеты в SQL
    if count_printers==1:
        sql.push_z_reports(z_reports_data)

    # --------------------------
    # ШАГ 4 Записываем сломанные ФИР в SQL
    if count_printers==1:
        sql.push_broken_fn(fiscal_broken, date_to)

    print ("Finished logs.")
    sys.stdout = old_stdout
    log_file.close()

    #part of HTML
    if count_printers==1:
        print("<br>================================<br>")
        print("Date from: ", date_from, "<br>")
        print("Date to: " ,date_to, "<br>")
        print("KKT number: ", fn[0], "<br><br>")
        print("--------------------------------<br>")
        print("|RECEIVING Z reports from OFD<br>")
        print("| TOTAL Z REPORTS: ", len(z_reports_data), "<br>")
        print("| TOTAL FN with Z REPORTS: ", count_printers, "<br>")
        print("| TOTAL BROKEN FN: ", len(fiscal_broken), "<br>")
        print("--------------------------------<br><br>")
        print("PROGRAM IS FINISHED")
    else:
        print("<br>================================<br>")
        print("Date from: ", date_from, "<br>")
        print("Date to: ", date_to, "<br>")
        print("KKT number: ", fn[0], "<br><br>")
        print("--------------------------------<br>")
        print('<font color="red">Please ATTENTION!<br>')
        print("It seems like wrong KKT number. Please check the number.<br>")
        print("</font>")
        print("--------------------------------<br><br>")


if __name__ == "__main__":
    main()
