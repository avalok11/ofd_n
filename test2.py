#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import auth as au
import validation as vl


def main():
    cooks, token = au.connect(vl.ofd_idd, vl.ofd_name, vl.ofd_pwd)
    print("\'cooks\':", cooks)
    print("\'token\':", token)


if __name__ == "__main__":
    main()