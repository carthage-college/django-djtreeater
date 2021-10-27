# -*- coding: utf-8 -*-

import os, sys
# env
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "djtreeater.settings.shell")

import django
django.setup()

from django.conf import settings

import argparse

import csv
import pysftp
import argparse
import logging

from djtreeater.sql.adirondack import ADIRONDACK_QUERY
from djtreeater.core.utilities import fn_write_student_bio_header
from djtreeater.core.utilities import fn_encode_rows_to_utf8

from djtools.utils.mail import send_mail
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
    Collect adirondack data for import
"""
# RawTextHelpFormatter method allows for new lines in help text
parser = argparse.ArgumentParser(
    description=desc, formatter_class=argparse.RawTextHelpFormatter
)
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

# set up our logger
logger = logging.getLogger('djtreeater')


def sftp_upload(upload_filename):
    # cnopts authorizes the program to ignore the host key
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None  # ignore known host key checking
    # sFTP connection information for Adironcack
    XTRNL_CONNECTION = {
        'host': settings.ADIRONDACK_HOST,
        'username': settings.ADIRONDACK_USER,
        'password': settings.ADIRONDACK_PASS,
        'port': settings.ADIRONDACK_PORT,
        'cnopts': cnopts
    }
    try:
        # print("Make Connection")
        with pysftp.Connection(**XTRNL_CONNECTION) as sftp:
            # change directory
            # sftp.chdir("test/in/")
            sftp.chdir("prod/in/")
            # print(upload_filename)
            sftp.put(upload_filename, preserve_mtime=True)
            # close sftp connection
            sftp.close()
    except Exception as e:
        SUBJECT = '[Adirondack] UPLOAD failed'
        BODY = 'Unable to PUT .txt file to adirondack server.\n\n{0}'.format(
            repr(e)
        )
        send_mail (
            None, [settings.ADIRONDACK_TO_EMAIL,], SUBJECT,
            settings.ADIRONDACK_FROM_EMAIL, 'email/default.html',
            BODY, [settings.ADMINS[0][1],]
        )
        logger.error(BODY)


def main():
    '''
    main function
    '''

    logger.error('skeletor is here')

    # Defines file names and directory location
    adirondackdata = ('{0}carthage_students.txt'.format(
        settings.ADIRONDACK_TXT_OUTPUT)
    )

    try:
        # set global variable
        global EARL
        # determines which database is being called from the command line
        if database == 'cars':
            EARL = settings.INFORMIX_ODBC
        elif database == 'train':
            EARL = settings.INFORMIX_ODBC_TRAIN
        else:
            print("database must be: 'cars' or 'train'")
            exit(-1)
        # --------------------------
        # Create the txt file
        # print(ADIRONDACK_QUERY)
        connection = get_connection(EARL)
        # connection closes when exiting the 'with' block
        with connection:
            data_result = xsql(
                ADIRONDACK_QUERY, connection, key=settings.INFORMIX_DEBUG
            ).fetchall()

        # print(data_result)
        ret = list(data_result)
        if ret is None:
            SUBJECT = "[Adirondack] Application failed"
            BODY = "SQL Query returned no data."
            send_mail (
                None, [settings.ADIRONDACK_TO_EMAIL,], SUBJECT,
                settings.ADIRONDACK_FROM_EMAIL, 'email/default.html',
                BODY, [settings.ADMINS[0][1],]
            )
        else:
            fn_write_student_bio_header()
            # print("Query successful")
            # print(ret)
            with open(adirondackdata, 'w') as file_out:
                csvWriter = csv.writer(file_out, delimiter='|')
                encoded_rows = fn_encode_rows_to_utf8(ret)
                for row in encoded_rows:
                # for row in ret:
                #     print(row)
                    csvWriter.writerow(row)
            file_out.close()

            if not test:
                print("Send to FTP")
                # send file to SFTP Site..
                sftp_upload(adirondackdata)

        if test:
            SUBJECT = "[Adirondack] Student Bio data success"
            BODY = "Retreieved data and sent it via SFTP to the eater of trees."
            send_mail(
                None, [settings.ADIRONDACK_TO_EMAIL,], SUBJECT,
                settings.ADIRONDACK_FROM_EMAIL, 'email/default.html',
                BODY, [settings.ADMINS[0][1],]
            )
            print('done')
            logger.error(BODY)

    except Exception as e:
        # print(str(e))
        logger.error("Error in adirondack student_bio.py, Error = " + repr(e))
        SUBJECT = '[Adirondack] Application Error'
        BODY = "Error in adirondack student_bio.py, Error = " + repr(e)
        send_mail (
            None, [settings.ADIRONDACK_TO_EMAIL,], SUBJECT,
            settings.ADIRONDACK_FROM_EMAIL, 'email/default.html', BODY,
            [settings.ADMINS[0][1],]
        )


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

    sys.exit(main())
