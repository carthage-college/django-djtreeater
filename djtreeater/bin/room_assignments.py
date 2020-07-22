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
import pyodbc
import django

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
parser.add_argument(
    "-run_mode",
    help="Manual or scheduled run?",
    dest="run_mode"
)


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
        python room_assignments.py --database=train --test -run_mode=auto
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

        if run_mode == "manual":
            # print("Manual Mode")
            session = input("Enter target session (EX. RA 2019):  ")
            hall = fn_translate_bldg_for_adirondack(input("Enter Hall code  "
                                            "- use ALL or specifec bldg: "))
            posted = input("Do you want unposted or posted records?  "
                               "Enter 0 for unposted, 1 for posted, "
                               "2 for changed, 0,2 for both: ")
            # print(hall)

        elif run_mode == "auto":

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
                    # print(row[0])
                    session = row[0]
                    hall = ''
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
                  +  "HALLCODE=" + hall \
                  + "&" + \
                  "Posted=" + posted
        # \
        #            + "&" \
        #            "STUDENTNUMBER=" + "1601033"
        # # "CurrentFuture=-1" + "&" \
        #                      "Ghost=0" + "&" \
        # NOTE:  HALLCODE can be empty
        # + "&" \
        # "HallCode=" + 'SWE'
         #        + "&" \
         #          "HALLCODE=" + hall \
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


        # print("URL = " + url)
        # print("______")
        i = 1
        while i < 5:
            # print(i)
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
                    print ("Http Error:", err)
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
            print("No new data found")
            pass
        else:
            # print(x['DATA'])
            room_file = settings.ADIRONDACK_TXT_OUTPUT + \
                        settings.ADIRONDACK_ROOM_ASSIGNMENTS + '.csv'
            room_archive = settings.ADIRONDACK_ROOM_ARCHIVED + \
                           settings.ADIRONDACK_ROOM_ASSIGNMENTS + \
                           datetimestr + '.csv'

            if os.path.exists(room_file):
                os.rename(room_file, room_archive)

            room_data = fn_encode_rows_to_utf8(x['DATA'])
            # print("__room data ____")
            # print(room_data)
            # Write header
            try:
                notify_flag = False
                fn_write_assignment_header(room_file)
                with open(room_file, 'a') as room_output:
                    for i in room_data:
                        try:
                            # print("__ i LOOP ____")
                            if i[0] is None:
                                print("No ID")
                                pass
                            else:
                                # print(i)

                                carthid = i[0]
                                bldgname = i[1]
                                adir_hallcode = i[2]
                                floor = i[3]
                                bed = i[5]
                                room_type = i[6]
                                occupancy = i[7]
                                roomusage = i[8]
                                timeframenumericcode = i[9]
                                """Note: Checkout date is returning in the checkout
                                  field from the API rather than checkoutdate field"""
                                checkin = i[10]
                                checkedindate = i[10]
                                checkout = i[12]
                                checkedoutdate = i[13]
                                po_box = i[14]
                                po_box_combo = i[15]
                                canceled = i[16]
                                canceldate = i[17]
                                cancelnote = i[18]
                                cancelreason = i[19]
                                ghost = i[20]
                                posted = i[21]
                                roomassignmentid = i[22]
                                sess = i[9][:2]
                                year = i[9][-4:]
                                term = i[9]
                                # occupants = i[7]
                                # print(i[2])
                                bldg = fn_fix_bldg(i[2])
                                billcode = fn_get_bill_code(carthid, str(bldg),
                                                            room_type,
                                                            roomassignmentid,
                                                            session, API_server,
                                                            key)
                                # print(billcode)
                                '''
                                Intenhsg can be: 
                                R = Resident, O = Off-Campus, C = Commuter
                                This routine is needed because the adirondack
                                hall codes match to multiple descriptions and
                                hall descriptions have added qualifiers such as
                                FOFF, MOFF, UNF, LOCA that are not available
                                elsewhere using the API.  Have to parse it to
                                assign a generic room
                                For non residents, we have a generic room for
                                CX and a dummy room on the Adirondack side
                                So we need two variables, on for Adirondack and
                                one for CX.
                                '''
                                adir_room = i[4]

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

                                # print("write room output")
                                csvwriter = csv.writer(room_output,
                                                       quoting=csv.QUOTE_NONNUMERIC
                                                       )
                                '''Need to write translated fields if csv is to
                                   be created'''
                                csvwriter.writerow([carthid, bldgname, bldg,
                                                    floor, room, bed, room_type,
                                                    occupancy, roomusage,
                                                    timeframenumericcode, checkin,
                                                    checkedindate, checkout,
                                                    checkedoutdate, po_box,
                                                    po_box_combo, canceled,
                                                    canceldate, cancelnote,
                                                    cancelreason, ghost, posted,
                                                    roomassignmentid, billcode])

                                '''
                                Validate if the stu_serv_rec exists first
                                update stu_serv_rec id, sess, yr, rxv_stat,
                                intend_hsg, campus, bldg, room, bill_code
                                '''

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
                                if len(ret) != 0:
                                    # if ret is not None:
                                    # print("Stu Serv Rec Found")
                                    # print(billcode)
                                    if billcode != 0:
                                        """compare rsv_stat, intend_hsg, bldg, room,
                                        billcode -- Update only if something has 
                                        changed"""
                                        # print("Record found " + carthid)

                                        for row in ret:

                                            if row[3] != rsvstat \
                                                    or row[4] != intendhsg \
                                                    or row[6] != bldg \
                                                    or row[7] != room \
                                                    or row[10] != billcode:

                                                # print("Need to update stu_serv_rec")
                                                q_update_stuserv_rec = '''
                                                    UPDATE stu_serv_rec 
                                                    set rsv_stat = ?,
                                                    intend_hsg = ?, campus = ?,
                                                    bldg = ?, room = ?,
                                                    bill_code = ?
                                                    where id = ? and sess = ? and
                                                    yr = ?'''
                                                q_update_stuserv_args = (rsvstat,
                                                                         intendhsg,
                                                                         "MAIN", bldg,
                                                                         room,
                                                                         billcode,
                                                                         int(carthid),
                                                                         sess,
                                                                         int(year))
                                                connection = get_connection(EARL)
                                                # print(q_update_stuserv_rec)
                                                # print(q_update_stuserv_args)
                                                """ connection closes when exiting the 
                                                                   'with' block """

                                                with connection:
                                                    cur = connection.cursor()
                                                    cur.execute(q_update_stuserv_rec,
                                                                q_update_stuserv_args)
                                                connection.commit()
                                                # connection.close()
                                                # continue
                                                """If anything is written to database
                                                    set this flag to True"""
                                                notify_flag = True

                                                print("Mark room as posted...")
                                                fn_mark_room_posted(carthid,
                                                        adir_room,
                                                        adir_hallcode,
                                                        term, posted,
                                                        roomassignmentid,
                                                        API_server, key)
                                            else:
                                                # print("No change needed in "
                                                #        "stu_serv_rec")
                                                print("Mark room as posted...")
                                                fn_mark_room_posted(carthid, adir_room,
                                                        adir_hallcode, term,
                                                        posted,
                                                        roomassignmentid,
                                                        API_server, key)
                                    else:
                                        # print("Bill code not found")
                                        fn_write_error(
                                            "Error in room_assignments.py - "
                                            "Bill code not found  ID = " + carthid,
                                            + ", Building = " + str(bldg) +
                                            ", Room assignment ID = "
                                            + str(roomassignmentid))
                                        fn_send_mail(settings.ADIRONDACK_TO_EMAIL,
                                                     settings.ADIRONDACK_FROM_EMAIL,
                                             "Error in room_assignments.py - "
                                             "Bill code not found  ID = " + carthid,
                                             + ", Building = " + str(bldg) +
                                             ", Room assignment ID = "
                                             + str(roomassignmentid),
                                             "Adirondack Error")
                                    # go ahead and update
                                else:
                                    """As of 1/30/20, we have decided that it
                                        makes sense to insert a skeleton
                                        stu_serv_rec here
                                        May need to deal with pulling from fall
                                        record for spring term, and deal with parking
                                        logic
                                        """
                                    q_create_stu_serv_rec = '''INSERT INTO 
                                            stu_serv_rec
                                            (id, sess, yr, rsv_stat, intend_hsg, 
                                            campus, bldg,  room, add_date, 
                                            bill_code)
                                        VALUES
                                            ({0},'{1}', {2}, '{3}', '{4}', '{5}', 
                                            '{6}', '{7}', '{8}','{9}')
                                    '''.format(carthid, sess, year, 'R', 'R',
                                               'MAIN', bldg, room, checkedindate,
                                               billcode)
                                    # print(q_create_stu_serv_rec)

                                    connection = get_connection(EARL)
                                    with connection:
                                        cur = connection.cursor()
                                        cur.execute(q_create_stu_serv_rec)
                                    connection.commit()

                                    fn_mark_room_posted(carthid,
                                                        room,
                                                        bldg,
                                                        term, posted,
                                                        roomassignmentid,
                                                        API_server, key)

                        except Exception as e:
                            print("Error in process " + repr(e))
                            fn_write_error("Error in room_assignments.py - file write: "
                                           + repr(e))
                            pass


                """Notify Student Billing of changes """
                # if run_mode == "auto":
                #     if notify_flag:
                #         # print("Notify Student accounts")
                #         fn_notify(room_file, EARL)
                # room_output.close()



            except Exception as e:
                print("Error in file write " + repr(e))
                fn_write_error("Error in room_assignments.py - file write: "
                               + repr(e))
                # fn_send_mail(settings.ADIRONDACK_TO_EMAIL,
                #              settings.ADIRONDACK_FROM_EMAIL,
                #              "Error in room_assignments.py - file write: "
                #              + repr(e), "Adirondack Error")
                pass
        # # Remove this after testing - only for testing when no
        # # recent changes are found via the API
        # room_file = settings.ADIRONDACK_TXT_OUTPUT + \
        #             settings.ADIRONDACK_ROOM_ASSIGNMENTS + '.csv'
        # if run_mode == 'auto':
        #     fn_notify(room_file, EARL)

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
    run_mode = args.run_mode

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

    if not run_mode:
        print("mandatory option missing: run_mode\n")
        parser.print_help()
        exit(-1)
    else:
        run_mode = run_mode.lower()

    if run_mode != 'manual' and run_mode != 'auto':
        print("run_mode must be: 'manual' or 'auto'")
        parser.print_help()
        exit(-1)

    if not test:
        test = 'prod'
    else:
        test = "test"

    sys.exit(main())

