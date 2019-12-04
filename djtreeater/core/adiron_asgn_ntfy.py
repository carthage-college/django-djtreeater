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
# from djtools.utils.mail import send_mail
from djimix.core.utils import get_connection, xsql

from djtreeater.sql.adirondack import ADIRONDACK_QUERY, Q_GET_TERM
from djtreeater.core.utilities import fn_get_bill_code, \
    fn_translate_bldg_for_adirondack, fn_write_error, fn_send_mail

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

def fn_get_name(id, EARL):
    fname = ""
    Q_GET_NAME = '''select fullname from id_rec 
       where id = {0}'''.format(id)

    connection = get_connection(EARL)
    # connection closes when exiting the 'with' block
    with connection:
        data_result = xsql(
            Q_GET_NAME, connection,
            key=settings.INFORMIX_DEBUG
        ).fetchall()
    ret = list(data_result)

    if ret is None:
        # print("Name not found")
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
        room_file = file
        # room_file = 'assignment.csv'
        r = csv.reader(open(room_file))
        """
        I am creating a room assignment csv on each day of changes and
        sending the bill code to CX
        Make comparisons based on that code in the csv file 
        after the assignment.csv is created, read the csv back into
        memory sorted by student ID and date
        Changes will have a posted code of 2 and a second record coded 0
        New entries will only have one record coded 0
        Loop through that set and determine if a room has changed and the
        bill code has also changed.
        """
        code_list = []
        xtra_list = []

        lastid = 0
        lastpost = 0
        lastdate = ""
        lastroom = ""
        lastroomtype = ""
        lastbldg = ""
        lastcode = ""
        fullname = ""

        for line in sorted(r, key=itemgetter(0, 10)):

            """Skip the header line - because of sort, it may not be 
            the first line in the record"""
            if not line[0].isnumeric():
                # print("Skip header")
                # if line[0] == "STUDENTNUMBER":
                pass

            else:
                post = line[21]
                # print("last id = " + str(lastid))
                """Revision 11/21/19 - records can come as a change, which
                  pulls two records from the API, or an add, which pulls
                  only one.   An add needs to be accounted for as a code
                  change, so I've reworked the logic"""

                if post == "2":
                    # print("First record of change")
                    """Same student, different row, change
                        write the record to the notification
                        set the passcount to 2
                    """
                elif post == "0":
                    if line[0] == lastid:
                        """this is the second record of a change"""
                        if lastcode == line[23]:
                            code_list.append("Student " + line[0]
                                    + ", " + fullname + " moved to "
                                    + line[2] + " " + line[4] + " " + line[23]
                                    + ", " + line[6] + " from " + lastbldg
                                    + " " + lastroom + " "
                                    + lastcode + ", " + lastroomtype
                                    + " on " + line[10])
                        else:
                            xtra_list.append("Student " + line[0]
                                    + ", " + fullname + " moved to " + line[2]
                                    + " " + line[4] + " " + line[23] + ", "
                                    + line[6] + " from " + lastbldg + " "
                                    + lastroom + " " + lastcode + ", "
                                    + lastroomtype + " on " + line[10])

                elif line[0] != lastid:
                    """The first assignment   """
                    fullname = fn_get_name(lastid, EARL)
                    code_list.append("Student " + lastid
                                     + ", " + fullname
                                     + " initially assigned to "
                                     + lastbldg + " " + lastroom + " "
                                     + lastcode + ", " + lastroomtype
                                     + " on " + lastdate)

                # store this row and go to the next record
                lastid = line[0]
                lastdate = line[10]
                lastroomtype = line[6]
                lastcode = line[23]
                lastbldg = line[2]
                lastroom = line[4]

        """PREPARE THE EMAIL"""
        if len(code_list) == 0:
            # print("No changes in file")
            quit()
        else:
            body = "\n" + "Room changes requiring bill code change:" + "\n"
            for i in code_list:
                body = body + i + "\n"

            if xtra_list is not None:
                body = body + "\n" + "Room changes not affecting bill code:" \
                       + "\n"
                for i in xtra_list:
                    body = body + i + "\n"

            frum = settings.ADIRONDACK_FROM_EMAIL
            tu = settings.ADIRONDACK_ASCII_EMAIL
            # tu = 'dsullivan@carthage.edu'
            subj = "Adirondack - Room Bill Code Change"

            fn_send_mail(tu, frum, body, subj)

            # print("Mail Sent " + subj + " TO:" + str(tu) + " FROM:" + str(frum)
            #       + " DETAILS: " + "\n" + body)


    except Exception as e:
        print(
                "Error in assign_notify.py:  " + repr(e))
        # fn_write_error(
        #     "Error in assign_notify.py:" + repr(e))


# def main():
#     EARL = settings.INFORMIX_ODBC_TRAIN
#     room_file = settings.ADIRONDACK_TXT_OUTPUT + \
#                 settings.ADIRONDACK_ROOM_ASSIGNMENTS + '.csv'
#     fn_notify(room_file, EARL)
#
# if __name__ == "__main__":
#
#     sys.exit(main())

