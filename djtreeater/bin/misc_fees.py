# -*- coding: utf-8 -*-
import os
import shutil
import sys
import time
import datetime as dt
from datetime import datetime
import codecs
import hashlib
import json
import requests
import csv
import argparse
import logging
import django
import string

# from string import maketrans
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
    fn_mark_room_posted, fn_translate_bldg_for_adirondack, \
    fn_mark_bill_exported, fn_set_terms, fn_check_cx_records, \
    fn_sendmailfees, fn_set_grad_terms, fn_sendmailfees_all_trms
from djtreeater.core.lookuplist import fn_lookuplist

from djimix.core.utils import get_connection, xsql

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

def main():
    # set global variable
    global EARL
    # determines which database is being called from the command line
    if database == 'cars':
        EARL = settings.INFORMIX_ODBC
    if database == 'train':
        EARL = settings.INFORMIX_ODBC_TRAIN
    else:
        # # this will raise an error when we call get_engine()
        # below but the argument parser should have taken
        # care of this scenario and we will never arrive here.
        EARL = None
    # establish database connection

    """To run:   python misc_fees.py --database=train --test  """

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

        # print("GET TERMS")
        # Adirondack dataset
        bill_list = []

      
        termlist = fn_lookuplist(test)
        # print("From THD Export Date")
        # print(termlist)

        if termlist:

            """
                        Cleanup previous run CSV files
                          """
            files = os.listdir(settings.ADIRONDACK_TXT_OUTPUT)
            for f in files:
                ext = f.find(".csv")
                if (f.startswith("2010") or f.startswith("2011")
                        or f.startswith("2031") or f.startswith("2040")):
                    shutil.move(settings.ADIRONDACK_TXT_OUTPUT + f,
                                settings.ADIRONDACK_TXT_OUTPUT +
                                "ascii_archive/" +
                                f[:ext] + "_" + timestr + f[ext:])


        # for row in ret:
        for row in termlist:
            # i = row[0].strip() + ' ' + str(row[1])
            print(row)
            adirondack_term = row
            # adirondack_term = i
            """Get data from Adirondack"""

            url = "https://carthage.datacenter.adirondacksolutions.com/" \
                +API_server+"/apis/thd_api.cfc?" \
                "method=studentBILLING&" \
                "Key=" + key \
                + "&" + "utcts=" + str(utcts) \
                + "&" + "h=" + hash_object.hexdigest() \
                + "&" + "TIMEFRAMENUMERICCODE=" + adirondack_term \
                + "&" + "AccountCode=2010,2040,2011,2031" \
                + "&" + "Exported=0"
                # + "&" + "STUDENTNUMBER=1566304"

            """
            DEFINIIONS
            Exported: -1 exported will be included, 0 only non-exported
            ExportCharges: if -1 then charges will be marked as exported
            DO NOT mark exported here.  Wait for later step
            """
            print("URL = " + url)

            response = requests.get(url)
            x = json.loads(response.content)

            """
            Make sure no duplicate records get into the system
            Use the STUDENTBILLINGINTERNALID number - uniquie row id for
            each adirondack billing entry
            Store the numbers in a txt file
            Read that file into a list and
            IF the new data pulls the same ID number, pass through
            """

            """ ------------------------------------------
               Step 1 would be to build the list of items already written to
               a csv for the terms
            ------------------------------------------"""

            # Set up the file names for the duplicate check
            cur_file = settings.ADIRONDACK_TXT_OUTPUT + "billing_logs/" + \
                       adirondack_term.replace(" ","") + '_processed.csv'
            # cur_file = settings.ADIRONDACK_TXT_OUTPUT + 'billing_logs/' +
            # current_term + '_processed.csv'
            # last_file = settings.ADIRONDACK_TXT_OUTPUT
            #     last_term + '_processed.csv'

            """Initialize a list of record IDs -- previously processed rows"""
            the_list = []

            """ Make sure file for the current term has been created"""
            if os.path.isfile(cur_file):
                # print ("cur_file exists")
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

                ffile.close()

            else:
                # print ("No file")
                fn_write_billing_header(cur_file)

            """
            Step 2 would be to loop through the new charges returned
            from adirondack in the API query
            """

            """
            Note.  Each account code must be a separate file for ASCII Post
            2010  Improper Checkout
            2011  Extended stay charge
            2031   Recore
            2040  Lockout fee
            Room rental fees are not for ASCII post and will not be
            calculated in Adirondack
            """

            for i in x['DATA']:
                # As the csv is being created
                # Compare each new file's line ID
                adir_term = i[4]
                ascii_term = i[4][:2] + i[4][-2:]


                # Round the amount to 2 decimal places
                amount = '{:.2f}'.format(i[2])
                bill_id = str(i[16])
                bill_list.append(bill_id)
                stu_id = str(i[0])
                item_date = i[1][-4:] + "-" + i[1][:2] + "-" + i[1][3:5]
                tot_code = str(i[6])
                item_type = i[13]

                if adirondack_term == adir_term:
                    """ here we look for a specific item"""

                    """ FORMAT DATE FOR SQL"""
                    chk_date = datetime.strptime(item_date, '%Y-%m-%d')
                    new_date = datetime.strftime(chk_date, '%m/%d/%Y')

                    """Make sure this charge is not already in CX"""
                    x = fn_check_cx_records(tot_code, adir_term, new_date,
                                            stu_id, amount, EARL)
                    if x == 0:
                        pass
                        # print("Item is not in CX database")
                    else:
                        print("WARNING:  Matching item exist in CX database")
                        continue

                    """Make sure item was not pulled previously"""
                    if int(bill_id) in the_list:
                        # print("Item " + bill_id + " already in list")
                        pass
                    else:
                        """Write the ASCII file and log the entry for
                           future reference"""
                        # print("Write to ASCII")
                        rec = []
                        rec.append(i[1])

                        """Limit to 26 characters just in case"""
                        tmpstr = str(i[5][:26])
                        descr = ''.join(filter(str.isalnum, tmpstr))
                        rec.append(descr.strip())
                        rec.append("1-003-10041")
                        rec.append('{:.2f}'.format(i[2]))
                        rec.append(stu_id)
                        rec.append("S/A")
                        rec.append(tot_code)
                        rec.append(ascii_term)

                        file_descr = item_type.replace(" ", "_")

                        fee_file = settings.ADIRONDACK_TXT_OUTPUT + tot_code \
                            + "_" + file_descr + "_" \
                                   + datetimestr + ".csv"

                        with open(fee_file, 'a') as fee_output:
                            csvwriter = csv.writer(fee_output)
                            csvwriter.writerow(rec)
                        fee_output.close()

                        """Write record of item to PROCESSED list"""
                        f = cur_file

                        with open(f, 'a') as wffile:
                            csvwriter = csv.writer(wffile)
                            csvwriter.writerow(i)
        # print(bill_list)
        files = os.listdir(settings.ADIRONDACK_TXT_OUTPUT)
        csv_exists = False
        fils = []

        """
        Each time I find a record it writes or appends to the file
        the file will therefore have today as the modified date
        Logic: if TODAY is equal to Last modified file data then 
        append that file to the list
        otherwise the file date would be older than today, meaning nothing 
        has changed
        if the program does not run, then the record will still be in 
        THD to pickup next time, it will make the change using TODAY
        as the date.   
        """

        for f in files:
            if (f.startswith("2010") or f.startswith("2011")
                    or f.startswith("2031") or f.startswith("2040")):
                last_modified = time.ctime(os.path.getmtime(settings.ADIRONDACK_TXT_OUTPUT + f))
                # print(last_modified)
                dtm = datetime.strptime(last_modified, "%a %b %d %H:%M:%S %Y")
                sdt = datetime.strftime(dtm, "%m/%d/%y")
                td = datetime.today()
                tds = datetime.strftime(td, "%m/%d/%y")
                # print(sdt)
                if tds == sdt:
                    fils.append(f)
                    csv_exists = True
        # print(fils)

        """Mark bill items as exported"""
        for bill_id in bill_list:
            # print(bill_id)
            fn_mark_bill_exported(bill_id, API_server, key)

        """When all done, email csv file"""
        """Needs to be outside the for loop"""
        if csv_exists == True:
            # print("EMAIL TO " + str(settings.ADIRONDACK_ASCII_EMAIL))
            subject = 'Housing Miscellaneous Fees'
            body = 'There are housing fees to process via ASCII ' \
                'post'

            fn_sendmailfees_all_trms(settings.ADIRONDACK_ASCII_EMAIL,
                            settings.ADIRONDACK_FROM_EMAIL,
                            body, subject
                            )


    except Exception as e:
        # print("Error in misc_fees.py - Main: "
        #                + repr(e))
        fn_write_error("Error in misc_fees.py - Main: "
                       + repr(e))

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
