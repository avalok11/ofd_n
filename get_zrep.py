#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import requests


def get_z_rep(cooks, reg_id, date_from, date_to, inn='7825335145'):
    """




    """
    response = requests.get('https://api.sbis.ru/ofd/v1/orgs/' + str(inn) + '/kkts/' + str(reg_id) + '/shifts?dateFrom='
                            + str(date_from) + '&dateTo=' + str(date_to) + '&accounting=byOpen&limit=1000',
                            cookies=cooks)
    return response.json()
