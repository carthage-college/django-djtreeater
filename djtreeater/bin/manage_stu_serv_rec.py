# -*- coding: utf-8 -*-

import os, sys
from datetime import datetime
# env
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "djtreeater.settings.shell")

import django
django.setup()

from django.conf import settings
from djimix.core.utils import get_connection, xsql

import argparse
import logging

logger = logging.getLogger('djtreeater')

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
Accepts as input...
"""
# RawTextHelpFormatter method allows for new lines in help text
parser = argparse.ArgumentParser(
    description=desc, formatter_class=argparse.RawTextHelpFormatter
 )

#
# parser.add_argument(
#     '-x', '--equis',
#     required=True,
#     help="Lorem ipsum dolor sit amet.",
#     dest='equis'
# )

parser.add_argument(
    '--test',
    action='store_true',
    help="Dry run?",
    dest='test'
)

parser.add_argument(
    "-d", "--database",
    help="database name.",
    dest="database"
)


def fn_set_term_vars():
    # this_yr = datetime.now().year
    # this_month = datetime.now().month

    this_yr = 2020
    this_month = 5

    # print("Current Month = " + str(this_month))
    # print("Current Year = " + str(this_yr))

    if this_month > 10 or this_month < 4:
        target_sess = 'RC'
        last_sess = 'RA'
    else:
        target_sess = 'RA'
        last_sess = 'RC'

    if this_month > 11 :
        target_yr = str(this_yr + 1)
        last_yr = str(this_yr)
    elif this_month < 4:
        target_yr = str(this_yr)
        last_yr = str(this_yr - 1)
    else:
        target_yr = str(this_yr)
        last_yr = str(this_yr)

    # print("Target Year = " + target_yr)
    # print("Target Sess = " + target_sess)
    #
    # print(last_sess)
    return [last_sess, last_yr, target_sess, target_yr]


def insert_ssr(id, sess, yr, bldg, room, billcode):

    '''Basic insert sql'''
    q_ins = '''insert into cx_sandbox:stu_serv_rec
        (id, sess, yr, rsv_stat, emer_phone, emer_phone_ext, emer_ctc_name, 
        offcampus_res_appr, intend_hsg, campus, bldg, room, suite, no_per_room, 
        asb_fee_wvd, campus_box, late_reg, hlth_ins_wvd, 
        meal_plan_type, meal_plan_wvd, res_asst, stat, 
        park_prmt_no, park_prmt_exp_date, park_location, 
        veh_type, veh_license, veh_lic_st, veh_year, veh_make, veh_model, 
        pref_rm_type, roommate_sts, crm_add_date, crm_upd_date, stusv_no, 
        ltr_sent, lot_no, add_date, bill_code, spec_flag, hous_wd_date, 
        with_reason)
    values
        ("{0}", {1}, {2}, "R", "", "", "", 
        "", "R", "MAIN", "{3}", "{4}", , 
        "", "", "", "", 
        "", "", "", "", 
        "", "", "", 
        "", "", "", 0, "", "", 
        "", "", "", "", "", 
        "", "", "", {5}, "", "", 
        "")'''.format(id, sess, yr, bldg, room, billcode )

    print(q_ins)
    return 1


def main():
    '''
    main function
    '''

    global EARL
    # if test:
    #     print("this is a test")
    #     logger.debug("debug = {}".format(test))
    # else:
    # print("this is not a test")
    # set global variable
    # determines which database is being called from the command line
    # if database == 'cars':
    #     EARL = settings.INFORMIX_ODBC_TRAIN
    if database == 'train':
        EARL = settings.INFORMIX_ODBC_TRAIN
    elif database == 'sandbox':
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

    """This will look for records in the stu_acad_rec that do not exist in
    the stu_serv_rec and will create a basic entry
    
    Incoming first time students will get a basic entry w/o bldg, room, parking,
    bill code, meal plan  
    Should start looking Nov 1 for upcoming spring term and March 15 for upcoming
    fall term
    
    Returning students will get a basic minimal entry for the fall term
    
    For the spring term, starting March 15 we will want to copy the info 
    from the fall term with the possible exception of the parking entry.  
    That gets billed only for the year, not for the second semester.
    
    """

    ret = fn_set_term_vars()
    print(ret)
    last_sess = ret[0]
    last_yr = ret[1]
    target_sess = ret[2]
    target_yr = ret[3]

    """This query finds those who don't have a stu_serv_rec"""
    cur_ssr_sql = '''select SAR.id, SAR.sess, SAR.yr, SAR.acst, SAR.cl, 
                SAR.cum_att_hrs, SAR.cum_earn_hrs 
                from stu_acad_rec SAR 
                where SAR.sess = "{0}"
                and SAR.yr = {1}
                and SAR.id not in (select id from stu_serv_rec 
                where sess = "{0}" 
                and yr = {1}) limit 2'''.format(target_sess, target_yr)
    print(cur_ssr_sql)

    connection = get_connection(EARL)
    """ connection closes when exiting the 'with' block """
    with connection:
        data_result = xsql(
            cur_ssr_sql, connection,
            key=settings.INFORMIX_DEBUG
        ).fetchall()
    cur_ssr = list(data_result)
    if len(cur_ssr) != 0:
        print("Stu Serv Rec Found")
        for row in cur_ssr:
            print(row)
            carth_id = row[0]
            stu_cl = row[4]
            """
            IF cl = 'FF' and 'cum_earn_hrs = 0
                Clean insert
            IF had a room last term...
                ???
            IF nothing from previous term 
                clean insert
            If fall term upcoming
                clean insert
            If spring term coming and fall term populated
                Copy fall to spring
        
            """
            if target_sess == 'RA' or stu_cl == 'FF':
                print("clean insert - no need to use last term")
                x = insert_ssr(carth_id, target_sess, target_yr, "", "", "")
                print(x)
            else:
                print("search previous term stu_serv_rec")
                """This query will find the prior stu_serv_rec if it exists"""
                last_ssr_sql = '''select id, sess, yr, intend_hsg, campus, bldg, 
                                    room, meal_plan_type, park_location, bill_code 
                                    from stu_serv_rec 
                                    where id = {0}
                                    and yr = {1}
                                    and sess = "{2}"'''.format(carth_id, last_yr,
                                                               last_sess)

                # print(last_ssr_sql)
                connection = get_connection(EARL)
                """ connection closes when exiting the 'with' block """
                with connection:
                    data_result = xsql(
                        last_ssr_sql, connection,
                        key=settings.INFORMIX_DEBUG
                    ).fetchall()
                    last_ssr = list(data_result)
                    if len(last_ssr) != 0:
                        print("Stu Serv Rec Found")
                        # for r in last_ssr:
                        print(r)
                        print ("Can use previous term")
                        billcode = r[9]
                        bldg = r[5]
                        room = r[6]
                        mealplan = r[7]
                        parkloc = r[8]

                        x = insert_ssr(carth_id, target_sess, target_yr, bldg,
                                       room, billcode)
                        print(x)
                    else:
                        print("No prior rec - insert clean")
                        x = insert_ssr(carth_id, target_sess, target_yr,
                                       "", "", "")
                        print(x)





######################
# shell command line
######################

if __name__ == '__main__':
    args = parser.parse_args()
    # equis = args.equis
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

    if test:
        print(args)

    sys.exit(main())

