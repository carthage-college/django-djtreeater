# -*- coding: utf-8 -*-
import os
import shutil
import sys
import time
import datetime
from datetime import datetime
import codecs
import hashlib
import json
import requests
import csv
import argparse
import logging
import django
# ________________
# Note to self, keep this here
# django settings for shell environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "djtreeater.settings.shell")
django.setup()
# ________________

from django.conf import settings
from djtreeater.core.utilities import fn_write_error, \
    fn_write_billing_header, fn_write_assignment_header, fn_get_utcts, \
    fn_encode_rows_to_utf8, fn_get_bill_code, fn_fix_bldg, \
    fn_mark_room_posted, fn_translate_bldg_for_adirondack

from djimix.core.utils import get_connection, xsql
# from djzbar.utils.informix import do_sql
# from djzbar.utils.informix import get_engine
# from djzbar.settings import INFORMIX_EARL_TEST
# from djzbar.settings import INFORMIX_EARL_PROD


# informix environment
os.environ['INFORMIXSERVER'] = settings.INFORMIXSERVER
os.environ['DBSERVERNAME'] = settings.DBSERVERNAME
os.environ['INFORMIXDIR'] = settings.INFORMIXDIR
os.environ['ODBCINI'] = settings.ODBCINI
os.environ['ONCONFIG'] = settings.ONCONFIG
os.environ['INFORMIXSQLHOSTS'] = settings.INFORMIXSQLHOSTS
os.environ['LD_LIBRARY_PATH'] = settings.LD_LIBRARY_PATH
os.environ['LD_RUN_PATH'] = settings.LD_RUN_PATH

# set up command-line options
desc = """
    Collect Handshake data for import
"""
parser = argparse.ArgumentParser(description=desc)

# Test with this then remove, use the standard logging mechanism
logger = logging.getLogger(__name__)

parser.add_argument(
    "--test",
    action='store_true',
    help="Dry run?",
    dest="test"
)
parser.add_argument(
    "-d", "--database",
    help="database name.",
    dest="database"
)

# Test with this then remove, use the standard logging mechanism
# logger = logging.getLogger(__name__)


def fn_check_cx_records(totcod, prd, jndate, stuid, amt):
    billqry = '''select  SA.id, IR.fullname, ST.subs_no, 
        SE.jrnl_date, ST.prd, ST.subs, STR.bal_code, ST.tot_code, SE.descr, 
        SE.ctgry, STR.amt, ST.amt_inv_act, SA.stat 
        from subtr_rec STR
        left join subt_rec ST on STR.subs = ST.subs
        and STR.subs_no = ST.subs_no 
        and STR.tot_code = ST.tot_code
        and STR.tot_prd = ST.prd
        left join sube_rec SE on SE.subs = STR.subs
        and SE.subs_no = STR.subs_no
        and SE.sube_no = STR.ent_no
        left join suba_rec SA on SA.subs = SE.subs
        and SA.suba_no = SE.subs_no
        left join id_rec IR on IR.id = SA.id
        where STR.subs = 'S/A'
        and STR.tot_code = "{0}"  
        and STR.tot_prd = "{1}"  
        and jrnl_date = "{2}"
        and IR.id = {3}
        and STR.amt = {4}
        '''.format(totcod, prd, jndate, stuid, amt)
    print(billqry)
    # ret = do_sql(billqry, earl=EARL)
    # print(ret)
    if ret is None:
        return 0
    else:
        return 1


def fn_set_terms(last_term, current_term):

    # Only RA and RC matter.
    # print(datetime.today().month)
    # print(str(datetime.today()))
    # If we are in spring RC term, last term will be RA with Year - 1
    # EX:  RC2020 current RA2019 last
    if datetime.today().month < 7:
        current_term = 'RC' + str(datetime.today().year)
        last_term = 'RA' + str(datetime.today().year - 1)
    # If we are in summer or fall both RA and RC will be current year
    # EX:  RA2019 current RC2019 last
    else:
        current_term = 'RA' + str(datetime.today().year)
        last_term = 'RC' + str(datetime.today().year)
    return [last_term, current_term]


def main():
    # set global variable
    global EARL
    # determines which database is being called from the command line
    if database == 'cars':
        EARL = settings.INFORMIX_ODBC_TRAIN
    if database == 'train':
        EARL = settings.INFORMIX_ODBC_TRAIN
    else:
        # # this will raise an error when we call get_engine()
        # below but the argument parser should have taken
        # care of this scenario and we will never arrive here.
        EARL = None
    # establish database connection

    # To run:   python misc_fees.py --database=train --test

    # print(test)
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

        datetimestr = time.strftime("%Y%m%d")
        timestr = time.strftime("%H%M")

        # Figure out what terms to limit to
        last_term, current_term = fn_set_terms('', '')
        # print("new last = " + last_term)
        # print("new current = " + current_term)

        # Terms in adirondack have a space between sess and year
        # print(current_term)
        adirondack_term = current_term[:2] + " " + current_term[2:]
        # print(adirondack_term)

        # ----------------------------------------
        # Get data from Adirondack
        # ----------------------------------------
        url = "https://carthage.datacenter.adirondacksolutions.com/" \
            +API_server+"/apis/thd_api.cfc?" \
            "method=studentBILLING&" \
            "Key=" + key \
            + "&" + "utcts=" + str(utcts) \
            + "&" + "h=" + hash_object.hexdigest() \
            + "&" + "TIMEFRAMENUMERICCODE=" + adirondack_term \
            + "&" + "AccountCode=2010,2040,2011,2031" \
            + "&" + "Exported=0"

            # + "&" + "STUDENTNUMBER=1501237"

        # DEFINIIONS
        # Exported: -1 exported will be included, 0 only non-exported
        # ExportCharges: if -1 then charges will be marked as exported
        # DO NOT mark exported here.  Wait for later step

        print("URL = " + url)

        response = requests.get(url)
        x = json.loads(response.content)
        if not x['DATA']:
            print("No new data found")
            pass
        else:
            # ----------------------------------------
            # Cleanup previous run CSV files
            # ----------------------------------------

            files = os.listdir(settings.ADIRONDACK_TXT_OUTPUT)
            for f in files:
                ext = f.find(".csv")
                if (f.startswith("2010") or f.startswith("2011")
                        or f.startswith("2031") or f.startswith("2040")):
                    shutil.move(settings.ADIRONDACK_TXT_OUTPUT + f,
                                settings.ADIRONDACK_TXT_OUTPUT +
                                "ascii_archive/" +
                                f[:ext] + "_" + timestr + f[ext:])

            # ----------------------------------------
            # Make sure no duplicate records get into the system
            # ----------------------------------------
            #    Use the STUDENTBILLINGINTERNALID number - uniquie row id for
            #    each adirondack billing entry
            #    Store the numbers in a txt file
            #    Read that file into a list and
            #    IF the new data pulls the same ID number, pass through

            # ------------------------------------------
            # Step 1 would be to build the list of items already written to
            # a csv for the terms
            # ------------------------------------------

            # Set up the file names for the duplicate check
            cur_file = settings.ADIRONDACK_TXT_OUTPUT + 'billing_logs/' + \
                current_term + '_processed.csv'
            last_file = settings.ADIRONDACK_TXT_OUTPUT + 'billing_logs/' + \
                last_term + '_processed.csv'

            # Initialize a list of record IDs
            the_list = []

            # Make sure file for the current term has been created
            if os.path.isfile(cur_file):
                fst = cur_file
                with open(fst, 'r') as ffile:
                    csvf = csv.reader(ffile)
                    # the [1:] skips header
                    # File should have at least columns for term row ID
                    next(ffile)
                    for row in csvf:
                        # This if statement traps for blank rows
                        # if not ''.join(row).strip():
                        assign_id = int(row[16].strip())
                        the_list.append(assign_id)
                        # print(the_list)

                ffile.close()

            else:
                print ("No file")
        #         fn_write_billing_header(cur_file)
        #
            # For extra insurance, include last term items in the list
            if os.path.isfile(last_file):
                # print ("last_file exists")
                lst = last_file
                with open(lst, 'r') as lfile:
                    csvl = csv.reader(lfile)  # the [1:] skips header
                    next(lfile)
                    for row in csvl:
                        # term = row[0]
                        assign_id = int(row[16].strip())
                        the_list.append(assign_id)
                lfile.close()
            else:
                print ("No file")
        #         fn_write_billing_header(last_file)

            # List of previously processed rows
            # print(the_list)

            # ------------------------------------------
            #  Step 2 would be to loop through the new charges returned
            #  from adirondack in the API query
            # ------------------------------------------

            # Note.  Each account code must be a separate file for ASCII Post
            # 2010  Improper Checkout
            # 2011  Extended stay charge
            # 2031   Recore
            # 2040  Lockout fee
            # Room rental fees are not for ASCII post and will not be
            # calculated in Adirondack

            # Adirondack dataset
            bill_list = []
        #
            for i in x['DATA']:
                # --------------------
                # As the csv is being created
                # Compare each new file's line ID

                # variables for readability
                adir_term = i[4][:2] + i[4][-4:]
                ascii_term = i[4][:2] + i[4][-2:]

                # Round the amount to 2 decimal places
                amount = '{:.2f}'.format(i[2])
                bill_id = str(i[16])
                bill_list.append(bill_id)

                # print(bill_id)
                # print(str(bill_list))

                stu_id = str(i[0])
                item_date = i[1][-4:] + "-" + i[1][:2] + "-" + i[1][3:5]
                tot_code = str(i[6])
                item_type = i[13]
                # print(item_type)

                # print("Adirondack term to check = " + adir_term)
                # print("CX Current Term = " + current_term)

                if current_term == adir_term:
                    # print("Match current term " + current_term)
                    # here we look for a specific item

                    # Make sure this charge is not already in CX
                    x = fn_check_cx_records(tot_code, adir_term, item_date,
                                            stu_id, amount)
                    if x == 0:
                        pass
                        # print("Item is not in CX database")
                    else:
                        # print("WARNING:  Matching item exist in CX database")
                        continue
                        # this will jump back to the start of the loop
                    # print(the_list)
                    # Make sure item was not pulled previously
                    if int(bill_id) in the_list:
                        # print("Item " + bill_id + " already in list")
                        pass
                    else:
                        # Write the ASCII file and log the entry for
                        # future reference
                        # print("Write to ASCII csv file")
                        rec = []
                        rec.append(i[1])
                        # Limit to 26 characters just in case
                        descr = str(i[5][:26])
                        descr = descr.translate(None, '!@#$%.,')
                        rec.append(descr.strip())
                        rec.append("1-003-10041")
                        # Round this?
                        rec.append('{:.2f}'.format(i[2]))
                        rec.append(stu_id)
                        rec.append("S/A")
                        rec.append(tot_code)
                        rec.append(ascii_term)

                        file_descr = item_type.replace(" ", "_")

                        fee_file = settings.ADIRONDACK_TXT_OUTPUT + tot_code \
                            + "_" + file_descr + "_" + datetimestr + ".csv"

                        with open(fee_file, 'ab') as fee_output:
                            csvwriter = csv.writer(fee_output)
                            csvwriter.writerow(rec)
                        fee_output.close()

                        # Write record of item to PROCESSED list
                        # print("Write item " + str(
                        #     i[16]) + " to current term file")
                        f = cur_file
                        # f = current_term + '_processed.csv'

                        with open(f, 'ab') as wffile:
                            csvwriter = csv.writer(wffile)
                            csvwriter.writerow(i)
                        wffile.close()

                else:
                    # In case of a charge from the previous term
                    # print(the_list)
                    # print("Match last term " + last_term)
                    if int(i[16]) in the_list:
                        pass
                        # print("Item " + str(i[16]) + " already in list")
                    else:
                        # print("Write to ASCII csv file")
                        rec = []
                        rec.append(i[1])
                        descr = str(i[5])
                        descr = descr.translate(None, '!@#$%.,')
                        rec.append(descr.strip())
                        rec.append("1-003-10041")
                        # Round this to two decimals
                        rec.append('{:.2f}'.format(i[2]))
                        rec.append(stu_id)
                        rec.append("S/A")
                        rec.append(tot_code)
                        rec.append(ascii_term)

                        file_descr = item_type.replace(" ", "_")

                        fee_file = settings.ADIRONDACK_TXT_OUTPUT + tot_code \
                            + "_" + file_descr + "_" + datetimestr + ".csv"

                        with open(fee_file, 'ab') as fee_output:
                            csvwriter = csv.writer(fee_output)
                            csvwriter.writerow(rec)
                        fee_output.close()

                        # Write record of item to PROCESSED list
                        # NOTE--QUOTE_MINIMAL is because timestamp has a comma
                        # print("Write item " + str(
                        #     i[16]) + " to current term file")

                        f = cur_file
                        # f = current_term + '_processed.csv'

                        with open(f, 'ab') as wffile:
                            csvwriter = csv.writer(wffile)
                            csvwriter.writerow(i)
                        wffile.close()

            files = os.listdir(settings.ADIRONDACK_TXT_OUTPUT)
            csv_exists = False
            for f in files:
                if (f.startswith("2010") or f.startswith("2011")
                        or f.startswith("2031") or f.startswith("2040")):
                    csv_exists = True

            # Mark bill items as exported
            for bill_id in bill_list:
                # print(bill_id)
                fn_mark_bill_exported(bill_id, API_server, key)

            # When all done, email csv file?
            # Ideally, write ASCII file to Wilson into fin_post directory
            if csv_exists == True:
                # print("File created, send")
                # print("EMAIL TO " + str(settings.ADIRONDACK_ASCII_EMAIL))
                subject = 'Housing Miscellaneous Fees'
                body = 'There are housing fees to process via ASCII ' \
                    'post'
                fn_sendmailfees('dsullivan@carthage.edu',
                                settings.ADIRONDACK_FROM_EMAIL,
                                body, subject
                                )
                # fn_sendmailfees(settings.ADIRONDACK_ASCII_EMAIL,
                #                 settings.ADIRONDACK_FROM_EMAIL,
                #                 body, subject
                #                 )

    except Exception as e:
        print("Error in misc_fees.py- Main:  " + e.message)
        # fn_write_error("Error in misc_fees.py - Main: "
        #                + e.message)

if __name__ == "__main__":
    args = parser.parse_args()
    test = args.test
    database = args.database

    if not database:
        print("mandatory option missing: database name\n")
        parser.print_help()
        exit(-1)
    else:
        database = database.lower()

    if database != 'cars' and database != 'train' and database != 'sandbox':
        print("database must be: 'cars' or 'train' or 'sandbox'\n")
        parser.print_help()
        exit(-1)

    if not test:
        test = 'prod'
    else:
        test = "test"

    sys.exit(main())
