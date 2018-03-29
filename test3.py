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


def main():
    cooks = au.connect(idd=vl.ofd_idd, login=vl.ofd_name, pwd=vl.ofd_pwd)
    reg_id = '0001603014030362'
    date_from = '2018-03-27T00:00:00'
    date_to = '2018-03-29T00:00:00'

    z_rep = z.get_z_rep(cooks, reg_id, date_from, date_to, inn='7825335145')

    print(z_rep)

    for zz in z_rep:
        print(zz)


if __name__ == "__main__":
    main()
