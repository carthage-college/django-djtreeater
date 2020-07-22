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
    fn_encode_rows_to_utf8, fn_fix_bldg, \
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
        csvwriter.writerow(["ID", "SESSION", "YEAR", "RSV_STAT", "CX RSV STAT",
                            "INTENDED HOUSING", "CX INTENDED HOUSING",
                            "ROOM_TYPE", "CHECK IN", "CHECK OUT","THD BLDG",
                            "CX BLDG", "THD ROOM", "CX ROOM",
                            "THD BILL CODE", "CX BILL CODE", "BILLID"])



def fn_get_bill_code(idnum, bldg, roomtype, roomassignmentid, session,
                     api_server, api_key):
    try:
        utcts = fn_get_utcts()
        hashstring = str(utcts) + api_key
        hash_object = hashlib.md5(hashstring.encode())
        url = "https://carthage.datacenter.adirondacksolutions.com/" \
            +api_server+"/apis/thd_api.cfc?" \
            "method=studentBILLING&" \
            "Key=" + api_key + "&" + "utcts=" + \
            str(utcts) + "&" + "h=" + \
            hash_object.hexdigest() + "&" + \
            "ASSIGNMENTID=" + str(roomassignmentid) + "&" + \
            "EXPORTED=0,-1"\
            # + "&" + \
            # "TIMEFRAMENUMERICCODE=" + session
            # __"STUDENTNUMBER=" + idnum + "&" + \

        # print(url)

        response = requests.get(url)
        x = json.loads(response.content)
        # print(len(x['DATA']))
        rowct = len(x['DATA'])
        if not x['DATA']:
            # print("No data")
            if bldg == 'CMTR':
                billcode = 'CMTR'
            elif bldg == 'OFF':
                billcode = 'OFF'
            elif bldg == 'ABRD':
                billcode = 'ABRD'
            else:
                billcode = ''
            return billcode
        else:
            # print(x['DATA'])
            # export_time = datetime.strptime("January, 01 1900 01:00:00", '%B, %d %Y %H:%M:%S')
            c = 0
            for  rows in x['DATA']:
                export_time = datetime.strptime(rows[10], '%B, %d %Y %H:%M:%S')
                # print(export_time)
                roomassignmentid = rows[14]
                billcode = rows[6]
                return billcode


                # THIS WOULD BE A CHANGE AND I ONLY WANT THE NEW RECORD
                # print("Posted = " + str(rows[9]))
                # print(rowct)
                # print(roomassignmentid)
                # if roomassignmentid == rows[14]:
                #     billcode = rows[6]
                #
                #     return billcode



    except Exception as e:
        fn_write_error("Error in utilities.py "
                       "- fn_get_bill_code: " + e.message)



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
                hall = ''
                posted = '1'
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
                  + "& GHOST=0"
                  # + "&" \
                  # "STUDENTNUMBER=" + "1374557"
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
            rowct = len(x['DATA'])
            print(rowct)
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
                    cancelreason = i[19]
                    checkin = i[10]
                    checkout = i[12]
                    ghost = i[20]
                    oldrectest = datetime.strptime(i[12], '%m/%d/%Y')
                    # print(cancelreason)
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
                        billcode = 'No Matching Billcode for ' \
                                   + str(roomassignmentid)

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

                    if posted == 2 and canceled != -1:
                        print("Record 1 of 2")
                        pass
                    else:

                        if posted == 2 and canceled == -1:
                            billcode = 'NOCH'


                        # Posted of 2 represents a change OR cancellation
                        # If not a cancellation, skip the record because
                        # there will be another record posted 0 with the correct
                        # bill record ID
                        if canceled == -1 and cancelreason == 'Withdrawal':
                            rsvstat = 'W'
                        else:
                            rsvstat = 'R'

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
                                cxroom = str(row[7])
                                cxbillcode = row[10]

                                if oldrectest < datetime.strptime('12/01/2020',
                                                                  '%m/%d/%Y'):
                                    oldrec = 'Old Record'
                                else:
                                    oldrec = ''

                                if ghost != 0:
                                    ghostrec = 'Ghost Record'
                                else:
                                    ghostrec = ""


                                # if csrsvstat != rsvstat \
                                #         or cxintendhsg != intendhsg \
                                if (cxbldg != bldg \
                                        or cxroom != room \
                                        or cxbillcode != billcode)\
                                        and ghostrec == "":

                                    print(carthid)

                                    with open("Compare.csv", 'a') as output:
                                        csvwriter = csv.writer(output)
                                        csvwriter.writerow([carthid, sess, year,
                                            rsvstat, csrsvstat,
                                            intendhsg, cxintendhsg, room_type,
                                            checkin, checkout,
                                            bldg, cxbldg, room, cxroom,
                                            billcode, cxbillcode,
                                            roomassignmentid, oldrec, ghostrec])



    except Exception as e:
        print(
                "Error in compare_systems.py- Main:  " +
                repr(e)) + str(carthid)
    #     fn_write_error("Error in compare_systems.py - Main: "
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

