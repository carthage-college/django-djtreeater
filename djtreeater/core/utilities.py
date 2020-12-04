import os
import csv
import json
import calendar
import time
import datetime
# from datetime import datetime
from datetime import date
import requests
import codecs
import hashlib
from time import strftime, strptime
import smtplib
import logging
from logging.handlers import SMTPHandler

# django settings for shell environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "djtreeater.settings.shell")

# django settings for script
from django.conf import settings
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

from djimix.core.utils import get_connection, xsql

# set up command-line options
desc = """
    Upload ADP data to CX
"""

# create logger
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)


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
        if not x['DATA']:
            print("No data")
            if bldg == 'CMTR':
                billcode = 'CMTR'
            elif bldg == 'RMTE':
                billcode = 'CMTR'
            elif bldg == 'OFF':
                billcode = 'OFF'
            elif bldg == 'ABRD':
                billcode = 'ABRD'
            else:
                billcode = ''
            return billcode
        else:
            for rows in x['DATA']:
                # print(roomassignmentid)
                if roomassignmentid == rows[14]:
                    billcode = rows[6]
                    return billcode
    except Exception as e:
        fn_write_error("Error in utilities.py "
                       "- fn_get_bill_code: " + e.message)


def fn_fix_bldg(bldg_code):
    if bldg_code[:3] == 'OAK':
        x = bldg_code.replace(" ", "")
        l = len(bldg_code.strip())
        b = bldg_code[l - 1:l]
        z = x[:3]
        bldg = z + b
        # print(bldg)
        return bldg
    else:
        return bldg_code


def fn_translate_bldg_for_adirondack(bldg_code):
    # Hall codes in Adirondack do not match CX, primarily OAKS
    # Allow both versions of the OAKS options
    switcher = {
        "OAK1": "OAKS1",
        "OAKS1": "OAKS1",
        "OAK2": "OAKS 2",
        "OAKS 2": "OAKS 2",
        "OAK3": "OAKS 3",
        "OAKS 3": "OAKS 3",
        "OAK4": "OAKS 4",
        "OAKS 4": "OAKS 4",
        "OAK5": "OAKS 5",
        "OAKS 5": "OAKS 5",
        "OAK6": "OAKS 6",
        "OAKS 6": "OAKS 6",
        "DEN": "DEN",
        "JOH": "JOH",
        "MADR": "MADR",
        "SWE": "SWE",
        "TAR": "TAR",
        "UN": "UN",
        "ABRD": "ABRD",
        "APT": "APT",
        "CMTR": "CMTR",
        "RMTE": "RMTE",
        "OFF": "OFF",
        "TOWR": "TOWR",
        "WD": "WD",
        "ALL": ""
    }
    return switcher.get(bldg_code, "Invalid Building")


def fn_mark_room_posted(stu_id, room_no, hall_code, term, posted,
                        roomassignmentid, api_server, api_key):
    try:
        utcts = fn_get_utcts()
        hashstring = str(utcts) + api_key
        hash_object = hashlib.md5(hashstring.encode())

        url = "https://carthage.datacenter.adirondacksolutions.com/" \
            +api_server+"/apis/thd_api.cfc?" \
            "method=housingASSIGNMENTS&" \
            "Key=" + api_key + "&" \
            "utcts=" + \
            str(utcts) + "&" \
            "h=" + hash_object.hexdigest() + "&" \
            "TimeFrameNumericCode=" + term + "&" \
            "Posted=" + str(posted) + "&" \
            "ROOMASSIGNMENTID=" + str(roomassignmentid) + "&" \
            "STUDENTNUMBER=" + str(stu_id) + "&" \
            "PostAssignments=-1"

        # DEFINITIONS
        # Posted: 0 returns only NEW unposted,
        #         1 returns posted, as in export out to our system
        #         2 changed or cancelled
        # PostAssignments: -1 will mark the record as posted.
        # CurrentFuture: -1 returns only current and future
        # Cancelled: -1 is for cancelled, 0 for not cancelled
        # Setting Ghost to -1 prevents rooms with no student from returning
        # print("URL = " + url)

        response = requests.get(url)
        x = json.loads(response.content)
        if not x['DATA']:
            fn_write_error("Unable to mark record as posted - "
                           "record not found")
        else:
            # print("Record marked as posted")
            pass

        """
           Because we are not using Adirondack for regular room rental fees
           we have no need of those billing records being active
           Any bills related to this assignment can be marked as posted
           Miscellaneous charges have an assignment ID of 0,
           """

        url = "https://carthage.datacenter.adirondacksolutions.com/" \
              + api_server + "/apis/thd_api.cfc?" \
              "method=studentBILLING&" \
              "Key=" + api_key + "&" \
              "utcts=" + str(utcts) + "&" \
              "h=" + \
              hash_object.hexdigest() + "&" \
              "STUDENTNUMBER=" + str(stu_id) + "&" \
              "ASSIGNMENTID=" + str(roomassignmentid) + "&" \
              "Exported=0" + "&" \
              "EXPORTCHARGES=-1"

        response = requests.get(url)
        x = json.loads(response.content)

    except Exception as e:
        fn_write_error("Error in utilities.py- fn_mark_room_posted: "
                       + e.message)


def fn_mark_bill_exported(bill_id, api_server, api_key):
    try:
        utcts = fn_get_utcts()
        hashstring = str(utcts) + api_key
        hash_object = hashlib.md5(hashstring.encode())

        url = "https://carthage.datacenter.adirondacksolutions.com/" \
            + api_server + "/apis/thd_api.cfc?" \
            "method=studentBILLING&" \
            "Key=" + api_key + "&" \
            "utcts=" + str(utcts) + "&" \
            "h=" + hash_object.hexdigest() + "&" \
            "STUDENTBILLINGINTERNALID=" + bill_id + "&" \
            "Exported=0" + "&" \
            "EXPORTCHARGES=-1"
        """ API Does not accept student ID as param if bill internal ID 
        is used """

        response = requests.get(url)
        x = json.loads(response.content)
        if not x['DATA']:
            # print("Unable to mark bill as exported - record not found")
            fn_write_error("Error in utilities.py- fn_mark_bill_exported: "
                           "Unable to mark as exported - record not "
                           "found in THD "
                           + e.message)
        else:
            # print("Bill marked as exported")
            pass
    except Exception as e:
        fn_write_error("Error in utilities.py- fn_mark_bill_exported: "
                       + e.message)


def fn_convert_date(ddate):
    if ddate != "":
        ndate = datetime.strptime(ddate, "%Y-%m-%d")
        retdate = datetime.strftime(ndate, "%m/%d/%Y")
    else:
        retdate = ''
    return retdate

def fn_write_misc_header():
    with codecs.open(settings.ADIRONDACK_ROOM_FEES, 'w') as fee_output:
        csvwriter = csv.writer(fee_output)
        csvwriter.writerow(["ITEM_DATE", "BILL_DESCRIPTION", "ACCOUNT_NUMBER",
                            "AMOUNT", "STUDENT_ID", "TOT_CODE", "BILL_CODE",
                            "TERM"])


def fn_write_billing_header(file_name):
    with open(file_name, 'w') as room_output:
        csvwriter = csv.writer(room_output)
        csvwriter.writerow(["STUDENTNUMBER", "ITEMDATE", "AMOUNT", "TIMEFRAME",
                            "TIMEFRAMENUMERICCODE", "BILLDESCRIPTION",
                            "ACCOUNT", "ACCOUNT_DISPLAY_NAME", "EFFECTIVEDATE",
                            "EXPORTED", "EXPORTTIMESTAMP", "BILLEXPORTDATE",
                            "TERMEXPORTSTARTDATE", "ITEMTYPE", "ASSIGNMENTID",
                            "DININGPLANID", "STUDENTBILLINGINTERNALID",
                            "USERNAME", " ADDITIONALID1"])


def fn_write_assignment_header(file_name):
    with open(file_name, 'w') as room_output:
        csvwriter = csv.writer(room_output)
        csvwriter.writerow(["STUDENTNUMBER", "HALLNAME", "HALLCODE", "FLOOR",
                            "ROOMNUMBER", "BED", "ROOM_TYPE", "OCCUPANCY",
                            "ROOMUSAGE",
                            "TIMEFRAMENUMERICCODE", "CHECKIN", "CHECKEDINDATE",
                            "CHECKOUT",
                            "CHECKEDOUTDATE", "PO_BOX", "PO_BOX_COMBO",
                            "CANCELED", "CANCELDATE",
                            "CANCELNOTE", "CANCELREASON", "GHOST", "POSTED",
                            "ROOMASSIGNMENTID", "CODE"])


def fn_write_application_header():
    with open(settings.ADIRONDACK_APPLICATONS, 'w') as output:
        csvwriter = csv.writer(output)
        csvwriter.writerow(["STUDENTNUMBER", "APPLICATIONTYPENAME",
                            "APP_RECEIVED", "APP_COMPLETE",
                            "TIMEFRAMENUMERICCODE", "ELECTRONIC_SIG_TS",
                            "CONTRACT_RECEIVED", "APP_CANCELED", "DEPOSIT",
                            "DEPOSIT_AMOUNT", "DEPOSIT_RECEIVED",
                            "PAYVENDORCONFIRMATION", "UNDERAGE",
                            "UNDERAGE_ELECTRONIC_SIG_TS", "INSURANCE_INTENT"
                            ])


def fn_write_student_bio_header():
    adirondackdata = ('{0}carthage_students.txt'.format(
        settings.ADIRONDACK_TXT_OUTPUT))

    with open(adirondackdata, 'w') as file_out:
        csvwriter = csv.writer(file_out, delimiter='|')
        csvwriter.writerow(
            ["STUDENT_NUMBER", "FIRST_NAME", "MIDDLE_NAME",
             "LAST_NAME", "DATE_OF_BIRTH", "GENDER",
             "IDENTIFIED_GENDER", "PREFERRED_NAME",
             "PERSON_TYPE", "PRIVACY_INDICATOR", "ADDITIONAL_ID1",
             "ADDITIONAL_ID2",
             "CLASS_STATUS", "STUDENT_STATUS", "CLASS_YEAR", "MAJOR",
             "CREDITS_SEMESTER",
             "CREDITS_CUMULATIVE", "GPA", "MOBILE_PHONE",
             "MOBILE_PHONE_CARRIER", "OPT_OUT_OF_TEXT",
             "CAMPUS_EMAIL", "PERSONAL_EMAIL", "PHOTO_FILE_NAME",
             "PERM_PO_BOX",
             "PERM_PO_BOX_COMBO", "ADMIT_TERM", "STUDENT_ATHLETE",
             "ETHNICITY", "ADDRESS1_TYPE", "ADDRESS1_STREET_LINE_1",
             "ADDRESS1_STREET_LINE_2", "ADDRESS1_STREET_LINE_3",
             "ADDRESS1_STREET_LINE_4", "ADDRESS1_CITY",
             "ADDRESS1_STATE_NAME", "ADDRESS1_ZIP", "ADDRESS1_COUNTRY",
             "ADDRESS1_PHONE",
             "ADDRESS2_TYPE", "ADDRESS2_STREET_LINE_1",
             "ADDRESS2_STREET_LINE_2", "ADDRESS2_STREET_LINE_3",
             "ADDRESS2_STREET_LINE_4", "ADDRESS2_CITY",
             "ADDRESS2_STATE_NAME", "ADDRESS2_ZIP", "ADDRESS2_COUNTRY",
             "ADDRESS2_PHONE",
             "ADDRESS3_TYPE", "ADDRESS3_STREET_LINE_1",
             "ADDRESS3_STREET_LINE_2", "ADDRESS3_STREET_LINE_3",
             "ADDRESS3_STREET_LINE_4", "ADDRESS3_CITY",
             "ADDRESS3_STATE_NAME", "ADDRESS3_ZIP", "ADDRESS3_COUNTRY",
             "ADDRESS3_PHONE",
             "CONTACT1_TYPE", "CONTACT1_NAME",
             "CONTACT1_RELATIONSHIP",
             "CONTACT1_HOME_PHONE",
             "CONTACT1_WORK_PHONE",
             "CONTACT1_MOBILE_PHONE",
             "CONTACT1_EMAIL",
             "CONTACT1_STREET",
             "CONTACT1_STREET2",
             "CONTACT1_CITY",
             "CONTACT1_STATE",
             "CONTACT1_ZIP",
             "CONTACT1_COUNTRY",
             "CONTACT2_TYPE", "CONTACT2_NAME",
             "CONTACT2_RELATIONSHIP", "CONTACT2_HOME_PHONE",
             "CONTACT2_WORK_PHONE", "CONTACT2_MOBILE_PHONE",
             "CONTACT2_EMAIL", "CONTACT2_STREET", "CONTACT2_STREET2",
             "CONTACT2_CITY", "CONTACT2_STATE", "CONTACT2_ZIP",
             "CONTACT2_COUNTRY", "CONTACT3_TYPE", "CONTACT3_NAME",
             "CONTACT3_RELATIONSHIP", "CONTACT3_HOME_PHONE",
             "CONTACT3_WORK_PHONE", "CONTACT3_MOBILE_PHONE",
             "CONTACT3_EMAIL", "CONTACT3_STREET", "CONTACT3_STREET2",
             "CONTACT3_CITY", "CONTACT3_STATE", "CONTACT3_ZIP",
             "CONTACT3_COUNTRY", "TERM", "RACECODE", "SPORT", "GREEK_LIFE"])
    file_out.close()


def fn_encode_rows_to_utf8(rows):
    encoded_rows = []
    for row in rows:
        try:
            encoded_row = []
            for value in row:
                #if isinstance(value, str):
                # value = value.decode('cp1252').encode("utf-8")
                encoded_row.append(value)
            encoded_rows.append(encoded_row)
        except Exception as e:
            print("Error in encoded_rows routine " + e.message)

            fn_write_error("Error in encoded_rows routine " + repr(e))
    return encoded_rows




def fn_write_error(msg):
    # create error file handler and set level to error
    handler = logging.FileHandler(
        '{0}adirondack_error.log'.format(settings.LOG_FILEPATH))
    handler.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(message)s',
                                  datefmt='%m/%d/%Y %I:%M:%S %p')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.error(msg)
    handler.close()
    logger.removeHandler(handler)
    fn_clear_logger()
    return "Error logged"


def fn_sendmailfees(to, frum, body, subject):
    # Create the message
    msg = MIMEMultipart()

    # email to addresses may come as list
    msg['To'] = to
    msg['From'] = frum
    msg['Subject'] = "From " + frum + " - " + subject
    # msg.add_header('reply-to', frum)
    msg['Reply-To'] = frum
    # By default, SMTP will always use smtp@carthage.edu as the from address
    # Reply to allows a different return email option
    # If the user clicks reply, it will bring up the From address

    text = ''

    # This can be outside the file collection loop
    msg.attach(MIMEText(body, 'csv'))

    files = os.listdir(settings.ADIRONDACK_TXT_OUTPUT)
    # filenames = []
    for f in files:
        # if f.find('misc_housing') != -1:
        if f.find('2010') != -1 or f.find('2011') != -1 \
                    or f.find('2031') != -1 or f.find('2040') != -1:
            # print(settings.ADIRONDACK_TXT_OUTPUT + f)
            part = MIMEBase('application', "octet-stream")
            part.set_payload(open(settings.ADIRONDACK_TXT_OUTPUT
                                  + f, "rb").read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition',
                            'attachment; filename="%s"' % os.path.basename(f))
            msg.attach(part)
            text = msg.as_string()
            # print(text)

    server = smtplib.SMTP('localhost')
    # show communication with the server
    try:
        server.sendmail(frum, to.split(','), text)

    finally:
        # server.quit()
        pass


def fn_sendmailfees_all_trms(to, frum, body, subject):
    # Create the message
    msg = MIMEMultipart()

    # print("IN UTILITY")
    # email to addresses may come as list
    msg['To'] = to
    msg['From'] = frum
    msg['Subject'] = "From " + frum + " - " + subject
    # msg.add_header('reply-to', frum)
    msg['Reply-To'] = frum
    # By default, SMTP will always use smtp@carthage.edu as the from address
    # Reply to allows a different return email option
    # If the user clicks reply, it will bring up the From address

    text = ''

    # This can be outside the file collection loop
    msg.attach(MIMEText(body, 'csv'))

    files = os.listdir(settings.ADIRONDACK_TXT_OUTPUT)
    # filenames = []
    for f in files:

        if f.find('2010') != -1 or f.find('2011') != -1 \
                or f.find('2031') != -1 or f.find('2040') != -1:
            last_modified = time.ctime(
                os.path.getmtime(settings.ADIRONDACK_TXT_OUTPUT + f))
            dtm = datetime.datetime.strptime(last_modified, "%a %b %d %H:%M:%S %Y")
            sdt = datetime.datetime.strftime(dtm, "%m/%d/%y")
            td = datetime.datetime.today()
            tds = datetime.datetime.strftime(td, "%m/%d/%y")
            if tds == sdt:
                part = MIMEBase('application', "octet-stream")
                part.set_payload(open(settings.ADIRONDACK_TXT_OUTPUT
                                      + f, "rb").read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition',
                                'attachment; filename="%s"' % os.path.basename(
                                    f))
                msg.attach(part)
                text = msg.as_string()

    server = smtplib.SMTP('localhost')
    # show communication with the server
    try:
        server.sendmail(frum, to.split(','), text)

    finally:
        # server.quit()
        pass



def fn_send_mail(to, frum, body, subject):
    """
    Stock sendmail in core does not have reply to or split of to emails
    --email to addresses may come as list
    """

    try:
        msg = MIMEText(body)
        msg['To'] = to
        msg['From'] = frum
        msg['Subject'] = subject
        txt = msg.as_string()

        # print("ready to send")
        server = smtplib.SMTP('localhost')
        # show communication with the server
        # if debug:
        #     server.set_debuglevel(True)
        # print(msg['To'])
        # print(msg['From'])
        server.sendmail(frum, to.split(','), txt)

    except Exception as e:
        print(
                "Error in utilities.py fn_send_mail:  " + repr(e))
        # fn_write_error(
        #     "Error in assign_notify.py:" + repr(e))

    finally:
        server.quit()
        # print("Done")
        pass


def fn_get_utcts():
    """GMT Zero hour is 1/1/70
    Zero hour in seconds = 0

    Current date and time"""
    a = datetime.datetime.now()
    """Format properly"""
    b = a.strftime('%a %b %d %H:%M:%S %Y')
    """convert to a struct time"""
    c = time.strptime(b)
    """Calculate seconds from GMT zero hour"""
    utcts = calendar.timegm(c)
    # print("Seconds from UTC Zero hour = " + str(utcts))
    return utcts


def fn_clear_logger():
    logging.shutdown()
    return "Clear Logger"

def fn_check_cx_records(totcod, prd, jndate, stuid, amt, EARL):
    try:
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
        # print(jndate)

        # print(billqry)
        # ret = do_sql(billqry, earl=EARL)
        # print(ret)

        # Get the current term
        # print(EARL)
        connection = get_connection(EARL)
        # connection closes when exiting the 'with' block
        # print("Connection established")
        with connection:
            data_result = xsql(
                billqry, connection,
                key=settings.INFORMIX_DEBUG
            ).fetchall()
        # print("Data returned")

        ret = list(data_result)
        # print(ret)

        # if ret is None:
        # if ret == []:
        if not ret:
                return 0
        else:
            return 1
    except Exception as e:
        print("Error in misc_fees.py - fn_check_cx_records:  " + repr(e))
        # fn_write_error("Error in misc_fees.py - Main: "
        #                + e.message)
        return 0


def fn_set_terms():
    # Only RA and RC matter.
    # print(datetime.today().month)
    # print(str(datetime.today()))
    # If we are in spring RC term, last term will be RA with Year - 1
    # EX:  RC2020 current RA2019 last
    if datetime.date.today().month < 7:
        current_term = 'RC' + str(datetime.date.today().year)
        last_term = 'RA' + str(datetime.date.today().year - 1)
    # If we are in summer or fall both RA and RC will be current year
    # EX:  RA2019 current RC2019 last
    else:
        current_term = 'RA' + str(datetime.date.today().year)
        last_term = 'RC' + str(datetime.date.today().year)
    return [last_term, current_term]


def fn_set_grad_terms():
    # Only RA and RC matter.
    # print(datetime.date.today().month)
    # print(str(datetime.today()))
    # If we are in spring RC term, last term will be RA with Year - 1
    # EX:  RC2020 current RA2019 last
    if datetime.date.today().month < 7:
        current_term = 'RC' + str(datetime.date.today().year)
        last_term = 'RA' + str(datetime.date.today().year - 1)
        current_term_gr = 'GC' + str(datetime.date.today().year)
        last_term_gr = 'GA' + str(datetime.date.today().year - 1)
    # If we are in summer or fall both RA and RC will be current year
    # EX:  RA2019 current RC2019 last
    else:
        current_term = 'RA' + str(datetime.date.today().year)
        last_term = 'RC' + str(datetime.date.today().year)
        current_term_gr = 'GA' + str(datetime.date.today().year)
        last_term_gr = 'GC' + str(datetime.date.today().year - 1)
    print([last_term, current_term, current_term_gr, last_term_gr])
    return [last_term, current_term, current_term_gr, last_term_gr]

