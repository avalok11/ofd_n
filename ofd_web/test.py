#!/usr/local/bin/python3.5
# -*- coding: UTF-8 -*-# enable debugging
import sys
import cgitb


def main():
    cgitb.enable()
    print("Content-Type: text/html;charset=utf-8")
    print()
    print("Hello World!<br>")

    print("Hello<br>")
    print("Start<br>")
    for param in sys.argv:
        param = param.split()
        print(param)
        print("<br>")
        for p in param:
            print(p)
            print("<br>")
    print("<br> End<br>")


if __name__ == "__main__":
    main()


