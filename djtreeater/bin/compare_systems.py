import hashlib
import json
from json.decoder import JSONDecodeError
import os
import sys
import time
import datetime
from datetime import datetime
from datetime import date
import requests
import csv
import argparse
import django
import urllib3
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# ________________
# Note to self, keep this here
# django settings for shell environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "djtreeater.settings.shell")
django.setup()
# ________________

from django.conf import settings
from djtools.utils.mail import send_mail
from djtreeater.sql.adirondack import Q_GET_TERM
from djtreeater.core.utilities import fn_write_error, \
    fn_write_billing_header, fn_write_assignment_header, fn_get_utcts, \
    fn_encode_rows_to_utf8, fn_get_bill_code, fn_fix_bldg, \
    fn_mark_room_posted, fn_translate_bldg_for_adirondack, fn_send_mail
from djtreeater.core.adiron_asgn_ntfy import fn_notify
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

# normally set as 'debug" in SETTINGS
DEBUG = settings.INFORMIX_DEBUG

# set up command-line options
desc = """
    Collect adirondack data Room assignments for stu_serv_rec
"""
parser = argparse.ArgumentParser(description=desc)

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


def fn_write_compare_header():
    with open("Compare.csv", 'w') as output:
        csvwriter = csv.writer(output)
        csvwriter.writerow(["ID", "SESSION", "YEAR",
                            "INTENDED HOUSING", "CX INTENDED HOUSING",
                            "THD BLDG", "CX BLDG", "THD ROOM", "CX ROOM",
                            "THD BILL CODE", "CX BILL CODE"])



def main():
    try:
        """
        Term will be RA + Current year
        One big push for returning students for RC term happens in 
        December
        Only returning will be in the system, no need to screen out 
        frosh

        Push again June 30 and July 30  for RC term (will include 
        frosh)
        Aug 1 start automation for fall term
        Stop automation for RC on last day of class - appr May 20

        May 1, June 30, July 30 December  for upcoming term

        From Aug to Dec, grab all RA current year
        From Jan to May 1 grab all RC current year
        On MAY 1, grab all RA current year
        on June 30 grab all RA current year
        On third wednesday in December grab all RC Next
        On Next day in Dec, go back to RA Current

        Only options are RC20xx and RA20xx, so I only need to determine
        which year to pass during each time frame.
        Question is, for spring housing, will both RA and RC need to be
        dealt with?
                """

        """
        This is the command needed to run the script
        python compare_systems.py --database=train --test 
        Must specify the database, whether testing or live and whether
        user input is required
        """

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

        if test != "test":
            API_server = "carthage_thd_prod_support"
            key = settings.ADIRONDACK_API_SECRET
        else:
            API_server = "carthage_thd_test_support"
            key = settings.ADIRONDACK_TEST_API_SECRET

        # print(API_server)
        # print(key)
        # print(EARL)

        utcts = fn_get_utcts()
        """Seconds from UTC Zero hour"""
        hashstring = str(utcts) + key

        """Assumes the default UTF-8"""
        hash_object = hashlib.md5(hashstring.encode())
        datetimestr = time.strftime("%Y%m%d%H%M%S")



        """Get the current term"""
        connection = get_connection(EARL)
        # connection closes when exiting the 'with' block
        with connection:
            data_result = xsql(
                Q_GET_TERM, connection,
                key=settings.INFORMIX_DEBUG
            ).fetchall()
        ret = list(data_result)
        # print(Q_GET_TERM)
        # print(ret)

        if ret is None:
            fn_write_error(
                "Error in room_assignments.py - Main: No term found ")
            fn_send_mail(settings.ADIRONDACK_TO_EMAIL,
                         settings.ADIRONDACK_FROM_EMAIL,
                         "Error in room_assignments.py - Main: No term "
                         "found ", "Adirondack Error")
            quit()
        else:
            for row in ret:
                print(row[0])
                session = row[0]
                hall = 'TOWR'
                posted = '0,2'
            """IMPORTANT! won't work if string has any spaces. NO SPACES"""


        url = "https://carthage.datacenter.adirondacksolutions.com/" \
              + API_server + "/apis/thd_api.cfc?" \
                  "method=housingASSIGNMENTS&" \
                  "Key=" + key + "&" \
                  "utcts=" + \
                  str(utcts) + "&" \
                  "h=" + hash_object.hexdigest() + "&" \
                  "TimeFrameNumericCode=" + session + "&" \
                  "Posted=" + posted \
                  + "&" \
                  "HALLCODE=" + hall \
        #            + "&" \
        #           "STUDENTNUMBER=" + "1490456"
        # # "CurrentFuture=-1" + "&" \
        #                      "Ghost=0" + "&" \
        # NOTE:  HALLCODE can be empty
        # + "&" \
        # "HallCode=" + 'SWE'
         # \
        '''
        DEFINITIONS
        Posted: 0 returns only NEW unposted,
        1 returns posted, as in out to our system
        2 changed or cancelled
        PostAssignments: -1 will mark the record as posted.
        CurrentFuture: -1 returns only current and future
        Cancelled: -1 is for cancelled, 0 for not cancelled

        'In theory, every room assignment in Adirondack should have
        a bill code'''

        print("URL = " + url)
        # print("______")

        i = 1

        while i < 5:
            try:
                response = requests.get(url)
                response.raise_for_status()
                # print("______")
                # print(response.content)
                x = json.loads(response.content)
                # print(x)
                # print(x['DATA'])
                i = 5
            except requests.exceptions.HTTPError as err:
                i += 1
                if i == 5:
                    print ("Http Error:", errh)
                    pass
            except requests.exceptions.ConnectionError as err:
                print ("Error Connecting:", err)
                pass
            except requests.exceptions.Timeout as err:
                print ("Timeout Error:", err)
                pass
            except requests.exceptions.RequestException as err:
                print ("OOps: Something Else", err)
                pass



        if not x['DATA']:
            # print("No new data found")
            pass
        else:
            room_data = fn_encode_rows_to_utf8(x['DATA'])
            # print("__room data ____")
            # print(room_data)
            # Write header
        #     try:
        #         notify_flag = False
            fn_write_compare_header()

            for i in room_data:
                # print("__ i LOOP ____")
                if i[0] is None:
                    print("No ID")
                    pass
                else:
                    # print(i)

                    carthid = i[0]
                    bldgname = i[1]
                    room_type = i[6]
                    canceled = i[16]
                    ghost = i[20]
                    posted = i[21]
                    roomassignmentid = i[22]
                    sess = i[9][:2]
                    year = i[9][-4:]
                    term = i[9]
                    bldg = fn_fix_bldg(i[2])
                    billcode = fn_get_bill_code(carthid, str(bldg),
                                                room_type,
                                                roomassignmentid,
                                                session, API_server,
                                                key)
                    # print(billcode)
                    if billcode == '':
                        billcode = 'No Matching Billcode for ' + roomassignmentid

                        # '''
        #                     Intenhsg can be:
        #                     R = Resident, O = Off-Campus, C = Commuter
        #                     This routine is needed because the adirondack
        #                     hall codes match to multiple descriptions and
        #                     hall descriptions have added qualifiers such as
        #                     FOFF, MOFF, UNF, LOCA that are not available
        #                     elsewhere using the API.  Have to parse it to
        #                     assign a generic room
        #                     For non residents, we have a generic room for
        #                     CX and a dummy room on the Adirondack side
        #                     So we need two variables, on for Adirondack and
        #                     one for CX.
        #                     '''

                    # print(bldg)
                    if bldg == 'CMTR':
                        intendhsg = 'C'
                        room = bldgname[(bldgname.find('_') + 1)
                                        - len(bldgname):]
                    elif bldg == 'OFF':
                        intendhsg = 'O'
                        room = bldgname[(bldgname.find('_') + 1)
                                        - len(bldgname):]
                    elif bldg == 'ABRD':
                        intendhsg = 'O'
                        room = bldgname[(bldgname.find('_') + 1)
                                        - len(bldgname):]
                    elif bldg == 'UN':
                        intendhsg = 'R'
                        room = bldgname[(bldgname.find('_') + 1)
                                        - len(bldgname):]
                    else:
                        intendhsg = 'R'
                        room = i[4]

                    if posted == 2 and canceled == -1:
                        billcode = 'NOCH'

                    if canceled == -1 and cancelreason == 'Withdrawal':
                        rsvstat = 'W'
                    else:
                        rsvstat = 'R'

        #                     print("write room output")
        #                     csvwriter = csv.writer(room_output,
        #                                            quoting=csv.QUOTE_NONNUMERIC
        #                                            )
        #                     '''Need to write translated fields if csv is to
        #                        be created'''
        #                     csvwriter.writerow([carthid, bldgname, bldg,
        #                                         floor, room, bed, room_type,
        #                                         occupancy, roomusage,
        #                                         timeframenumericcode, checkin,
        #                                         checkedindate, checkout,
        #                                         checkedoutdate, po_box,
        #                                         po_box_combo, canceled,
        #                                         canceldate, cancelnote,
        #                                         cancelreason, ghost, posted,
        #                                         roomassignmentid, billcode])
        #
        #                     '''
        #                     Validate if the stu_serv_rec exists first
        #                     update stu_serv_rec id, sess, yr, rxv_stat,
        #                     intend_hsg, campus, bldg, room, bill_code
        #                     '''
        #
                        q_validate_stuserv_rec = '''
                                      select id, sess, yr, rsv_stat,
                                      intend_hsg, campus, trim(bldg),
                                      trim(room),
                                      no_per_room,
                                      add_date,
                                      trim(bill_code), hous_wd_date
                                      from stu_serv_rec
                                      where yr = {2}
                                      and sess  = "{1}"
                                      and id = {0}'''.format(carthid,
                                                             sess, year)
    #
                        connection = get_connection(EARL)
                        # print(q_validate_stuserv_rec)
                        """ connection closes when exiting the 'with' block """
                        with connection:
                            data_result = xsql(
                                q_validate_stuserv_rec, connection,
                                key=settings.INFORMIX_DEBUG
                            ).fetchall()
                        ret = list(data_result)
                        # connection.close()
                        # print(ret)
                        for row in ret:

                            csrsvstat = row[3]
                            cxintendhsg = row[3]
                            cxbldg = row[6]
                            cxroom = row[7]
                            cxbillcode = row[10]

                            # if row[3] !=  rsvstat \
                            #         or row[4]
                            #         !=
                            #         intendhsg \
                            #         or row[6]
                            #         != bldg \
                            #         or row[7]
                            #         != room \
                            #         or row[10]
                            #         != billcode:

                            # print(carthid, sess, year, rsvstat, csrsvstat,
                            #       intendhsg, cxintendhsg, bldg, cxbldg,
                            #       room, cxroom,  billcode, cxbillcode,
                            #   roomassignmentid )

                            with open("Compare.csv", 'a') as output:
                                csvwriter = csv.writer(output)
                                csvwriter.writerow([carthid, sess, year,
                                    rsvstat, csrsvstat,
                                    intendhsg, cxintendhsg, bldg, cxbldg,
                                    room, cxroom,  billcode, cxbillcode,
                                    roomassignmentid])

        #                     if len(ret) != 0:
        #                         # if ret is not None:
        #                         print("Stu Serv Rec Found")
        #                         print(billcode)
        #                         if billcode != 0:
        #                             """compare rsv_stat, intend_hsg, bldg, room,
        #                             billcode -- Update only if something has
        #                             changed"""
        #                             print("Record found " + carthid)
        #
        #
        #

        #
        #
        #
        #     except Exception as e:
        #         print("Error in file write " + repr(e))
        #         fn_write_error("Error in room_assignments.py - file write: "
        #                        + repr(e))
        #         # fn_send_mail(settings.ADIRONDACK_TO_EMAIL,
        #         #              settings.ADIRONDACK_FROM_EMAIL,
        #         #              "Error in room_assignments.py - file write: "
        #         #              + repr(e), "Adirondack Error")


    except Exception as e:
        print(
                "Error in adirondack_room_assignments_api.py- Main:  " +
                repr(e))
    #     fn_write_error("Error in room_assignments.py - Main: "
    #                    + repr(e))


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

