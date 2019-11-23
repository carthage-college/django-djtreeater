import os
import sys
import csv
import argparse
import django
import mimetypes
import smtplib

# ________________
# Note to self, keep this here
# django settings for shell environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "djtreeater.settings.shell")
django.setup()
# ________________

from django.conf import settings
from email.mime.text import MIMEText
from djtools.utils.mail import send_mail
from djimix.core.utils import get_connection, xsql

from djtreeater.sql.adirondack import ADIRONDACK_QUERY, Q_GET_TERM
from djtreeater.core.utilities import fn_get_bill_code, \
    fn_translate_bldg_for_adirondack, fn_write_error

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
#
# def fn_send_mail(to, frum, body, subject):
#     # email to addresses may come as list
#     # Stock sendmail in core does not have reply to or split of to emails
#     msg = MIMEText(body)
#     msg['To'] = to
#     msg['From'] = frum
#     msg['Subject'] = subject
#     msg['Reply-To'] = frum
#
#     print("ready to send")
#     server = smtplib.SMTP('localhost')
#     # show communication with the server
#     # if debug:
#     #     server.set_debuglevel(True)
#     try:
#         # print(msg['To'])
#         # print(msg['From'])
#         server.sendmail(frum, to.split(','), msg.as_string())
#
#     finally:
#         server.quit()
#         # print("Done")
#         pass


def fn_get_name(id, EARL):
    fname = ""
    Q_GET_NAME = '''select fullname from id_rec 
       where id = {0}'''.format(id)

    # ret = do_sql(Q_GET_NAME, key=DEBUG, earl=EARL)
    connection = get_connection(EARL)
    # connection closes when exiting the 'with' block
    with connection:
        data_result = xsql(
            Q_GET_NAME, connection,
            key=settings.INFORMIX_DEBUG
        ).fetchall()
    ret = list(data_result)

    if ret is None:
        # row = ret.fetchone()
        # if row is None:
            print("Name not found")
            fn_write_error(
                "Error in asign_notify.py - fn_get namen: No "
                "name found ")
            quit()
            fname = str(id)
        else:
            for row in ret:
                fname = row[0]

    return fname

def fn_notify(file, EARL):
    try:
        from operator import itemgetter
        # This needs to be pointed to the d2 data folder
        room_file = file
        # room_file = 'assignment.csv'
        print(room_file)
        r = csv.reader(open(room_file))

        # I am creating a room assignment csv on each day of changes and
        # sending the bill code to CX
        # Can easily add bill code to the csv file and make comparisons based
        # on that code, the csv file is only used for history and auditing...
        # so after the assignment.csv is created, read the csv back into
        # memory sorted by student ID and date
        # Changes will have a
        # Loop through that set and determine if a room has changed and the
        # bill code has also changed.

        code_list = [""]
        xtra_list = [""]

        lastid = ""
        lastpost = 0
        lastdate = ""
        lastroom = ""
        lastroomtype = ""
        lastbldg = ""
        lastcode = ""
        fullname = ""
        for line in sorted(r, key=itemgetter(0, 10)):

            print(line[0] + " " + line[10] + " " +  str(line[21]))

            # Skip the first line
            if line[0] == "STUDENTNUMBER":
                pass
            elif line[0] != lastid:
                lastid = line[0]
                lastpost = line[21]
                lastdate = line[10]
                lastroomtype = line[6]
                lastcode = line[23]
                lastbldg = line[2]
                lastroom = line[4]
                print("first record " + lastid + " " + lastpost + " "
                  + lastdate + " " + lastroom + " " + lastcode)
            else:
                if line[6] != lastroomtype:
                    print("Change to " + line[23])
                    # go get student name here...
                    fullname = ""
                    Q_GET_NAME = '''select fullname from id_rec 
                       where id = {0}'''.format(line[0])

                    connection = get_connection(EARL)
                    # connection closes when exiting the 'with' block
                    with connection:
                        data_result = xsql(
                            Q_GET_NAME, connection,
                            key=settings.INFORMIX_DEBUG
                        ).fetchall()
                    ret = list(data_result)
                    # ret = do_sql(Q_GET_NAME, key=DEBUG, earl=EARL)

                    if ret is None:
                        print("Name not found")
                        # fn_write_error(
                        #     "Error in asign_notify.py - fn_notifyn: No "
                        #     "name found ")
                        quit()
                    else:
                        for row in ret:
                            fullname = row[0]

                    code_list.append("Student " + line[0]
                            + ", " + fullname + " moved to " + line[2] + " "
                            + line[4] + " " + line[23] + ", " + line[6]
                            + " from " + lastbldg + " " + lastroom + " "
                            + lastcode + ", " + lastroomtype
                            + " on " + line[10])
                else:
                    print("Not a change")
                    xtra_list.append("Student " + line[0]
                                     + ", " + fullname
                                     + " moved to " + line[2] + " " + line[4]
                                     + " " + line[23]
                                     + ", " + line[6]
                                     + " from "
                                     + lastbldg + " " + lastroom + " "
                                     + lastcode + ", " + lastroomtype
                                     + " on " + line[10])

        body = "\n" + "Room changes requiring bill code change:"
        for i in code_list:
            body = body + i + "\n"

        if xtra_list is not None:
            body = body + "\n" + "Room changes not affecting bill code:"
            for i in xtra_list:
                body = body + i + "\n"

        frum = settings.ADIRONDACK_FROM_EMAIL
        # # tu = settings.ADIRONDACK_TO_EMAIL
        tu = 'dullivan@carthage.edu'
        # # tu = settings.ADIRONDACK_ASCII_EMAIL  #has Marietta and Carol
        subj = "Adirondack - Room Bill Code Change"

        # mail not working as of 11/15/19 - debug
        # # fn_send_mail(tu, frum, body, subj)

        print("Mail Sent " + subj + " TO:" + str(tu) + " FROM:" + str(frum)
              + " DETAILS: " + "\n" + body)

    except Exception as e:
        print(
                "Error in assign_notify.py:  " +                 e.message)
        # fn_write_error(
        #     "Error in assign_notify.py:" + e.message)


# def main():
#     EARL = settings.INFORMIX_ODBC_TRAIN
#     room_file = settings.ADIRONDACK_TXT_OUTPUT + \
#                 settings.ADIRONDACK_ROOM_ASSIGNMENTS + '.csv'
#     fn_notify(room_file, EARL)
#
# if __name__ == "__main__":
#
#     sys.exit(main())

