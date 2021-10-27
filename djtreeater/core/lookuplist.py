# -*- coding: utf-8 -*-
import os
import sys
import datetime as dt
from datetime import datetime, date
import hashlib
import json
import requests
import argparse
import logging
import django

# Note to self, keep this here
# django settings for shell environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "djtreeater.settings.shell")
django.setup()
# ________________

from django.conf import settings
from djtreeater.core.utilities import fn_write_error, fn_get_utcts


    # set up command-line options
desc = """
    Collect Handshake data for import
"""
parser = argparse.ArgumentParser(description=desc)

# Test with this then remove, use the standard logging mechanism
logger = logging.getLogger(__name__)

# def main():
def fn_lookuplist(test):

    if test != "test":
        API_server = "carthage_thd_prod_support"
        key = settings.ADIRONDACK_API_SECRET
    else:
        API_server = "carthage_thd_test_support"
        key = settings.ADIRONDACK_TEST_API_SECRET
    # print(API_server)

    try:
        utcts = fn_get_utcts()
        hashstring = str(utcts) + key

        # Assumes the default UTF-8
        hash_object = hashlib.md5(hashstring.encode())


        '''Cannot pass in a timeframenumericcode at present,but can return 
        them.  May be the method to get all, filter by date and build a list
        based on end date > current date.  Can then use that list in other
        python scripts to determine what to bring back'''

        url = "https://carthage.datacenter.adirondacksolutions.com/" \
            +API_server+"/apis/thd_api.cfc?" \
            "method=LookUpList&" \
            "Key=" + key \
            + "&" + "utcts=" + str(utcts) \
            + "&" + "h=" + hash_object.hexdigest() \
            + "&" + "TableList=TimeFrame"
        # print("URL = " + url)

        response = requests.get(url)
        x = json.loads(response.content)

        termlist = []
        # print(x['DATA'])

        tday = datetime.today().strftime("%B, %d %Y %H:%M:%S")
        thisday = datetime.strptime(tday, "%B, %d %Y %H:%M:%S")

        for i in x['DATA']:
            startdate = datetime.strptime(i[1], "%B, %d %Y %H:%M:%S")
            enddate = datetime.strptime(i[2], "%B, %d %Y %H:%M:%S")
            exportdate = datetime.strptime(i[4], "%B, %d %Y %H:%M:%S")
            termcode = i[3]
            # print("Term = " + i[0])
            # print("End date = " + str(enddate))

            if exportdate is not None and termcode is not None:
                if exportdate < thisday and enddate > thisday:
                    # print(str(exportdate) + ', ' + termcode)
                    # termlist.append("(" + termcode + ', ' + str(exportdate) + ")")
                    termlist.append(termcode)
                    # print("Term = " + i[0])
                    # print("Export date = " + str(exportdate))
            else:
                pass
                # print("nada")
        return termlist

    except Exception as e:
        print("Error in lookuplist.py - Main: "
                       + repr(e))
        # fn_write_error("Error in lookuplist.py - Main: "
        #                + repr(e))

