#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import auth as au
import get_kkt as kkt
import validation as vl


def main():
    cooks = au.connect(vl.ofd_idd, vl.ofd_name, vl.ofd_pwd)
    print("\'cooks\':", cooks)

    list_kkt = kkt.get_kkt(cooks, inn='7825335145')
    print(list_kkt)

    for l in list_kkt:
        print(l)


if __name__ == "__main__":
    main()
