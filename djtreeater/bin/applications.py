# -*- coding: utf-8 -*-

import os
# django settings for shell environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "djequis.settings")
import django
django.setup()
from django.conf import settings

import calendar
import time
import datetime
import hashlib
import json
import requests
import csv

from djtreeater.core.utilities import fn_write_error, \
    fn_write_application_header, fn_get_utcts

# set up command-line options
desc = """
    Collect adirondack data from applications for housing
"""

def encode_rows_to_utf8(rows):
    encoded_rows = []
    for row in rows:
        try:
            encoded_row = []
            for value in row:
                if isinstance(value, basestring):
                    value = value.decode('cp1252').encode("utf-8")
                encoded_row.append(value)
            encoded_rows.append(encoded_row)
        except Exception as e:
            fn_write_error("Error in encoded_rows routine " + e.message)
    return encoded_rows


def main():
    try:

        utcts = fn_get_utcts()
        hashstring = str(utcts) + settings.ADIRONDACK_API_SECRET
        # print("Hashstring = " + hashstring)

        # Assumes the default UTF-8
        hash_object = hashlib.md5(hashstring.encode())
        print(hash_object.hexdigest())

        # sendtime = datetime.now()
        # print("Time of send = " + time.strftime("%Y%m%d%H%M%S"))

        # Will need to build URL dynamically based on input from user
        # Allow query by student ID
        # Include term as variable
        searchval_id = input("Enter Carthage ID:   ")
        print(searchval_id)
        searchval_term = input("Enter Term: (Example RA 2019):  " )
        print(searchval_term)

        url = "https://carthage.datacenter.adirondacksolutions.com/" \
            "carthage_thd_prod_support/apis/thd_api.cfc?" \
            "method=housingAPPLICATIONS&" \
            "Key=" + settings.ADIRONDACK_API_SECRET + "&" \
            "utcts=" + str(utcts) + "&" \
            "h=" + hash_object.hexdigest() + "&" \
            "TimeFrameNumericCode=" + searchval_term + "&" \
            "studentNumber=" + searchval_id
        # possible additional params
        # + "&"
        # "ApplicationTypeName= "
        # + "&"
        # "APP_COMPLETE= "
        # + "&"
        # "APP_CANCELED= "
        # + "&"
        # "DEPOSIT= "
        # + "&"
        # "UNDERAGE= "

        print("URL = " + url)

        response = requests.get(url)
        x = json.loads(response.content)
        # print(x)
        # y = (len(x['DATA'][0][0]))
        if not x['DATA']:
            print("No match")
        else:
            fn_write_application_header()

            print("Start Loop")
            with open(settings.ADIRONDACK_APPLICATONS, 'ab') as output:
                for i in x['DATA']:
                    print(i)
                    csvWriter = csv.writer(output,
                                           quoting=csv.QUOTE_NONNUMERIC)
                    csvWriter.writerow(i)


    except Exception as e:
        print("Error in adirondack_applicationss_api.py- Main:  " + e.message)
        # fn_write_error("Error in adirondack_std_billing_api.py - Main: "
        #                + e.message)


if __name__ == "__main__":
    main()
#     args = parser.parse_args()
#     test = args.test
# sys.exit(main())
