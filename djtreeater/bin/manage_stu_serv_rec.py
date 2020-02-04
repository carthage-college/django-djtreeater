# -*- coding: utf-8 -*-

import os, sys
from datetime import datetime
# env
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "djtreeater.settings.shell")

import django
django.setup()

from django.conf import settings
from djimix.core.utils import get_connection, xsql
from djtreeater.sql.stu_serv_rec import get_spring_to_fall, get_fall_to_spring, \
    insert_ssr, last_ssr
from djtreeater.core.stu_serv_utils import fn_set_term_vars

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
    #     EARL = settings.INFORMIX_ODBC
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

    """+++++++++++++++++++++++++++++++++++++++++++++++++"""
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
    print(EARL)

    ret = fn_set_term_vars()
    print(ret)
    last_sess = ret[0]
    last_yr = ret[1]
    target_sess = ret[2]
    target_yr = ret[3]

    """For Spring session, we need to collect info if it exists from the fall
        stu serv rec"""
    if target_sess == 'RC':
        cur_ssr_sql = get_fall_to_spring(target_sess, target_yr)

        """For Fall session, we do not need to know anything about the spring
       stu_serv_rec and there shouldn't be any First Time Frosh"""
    else:
        cur_ssr_sql = get_spring_to_fall(target_sess, target_yr)

    # print(cur_ssr_sql)

    connection = get_connection(EARL)
    """ connection closes when exiting the 'with' block """
    with connection:
        data_result = xsql(
            cur_ssr_sql, connection,
            key=settings.INFORMIX_DEBUG
        ).fetchall()
    cur_ssr = list(data_result)
    # print(cur_ssr)
    if len(cur_ssr) != 0:
        for row in cur_ssr:
            print('----------------')
            print("Stu Serv Rec Found for " + str(row[0]))
            print(row)
            carth_id = row[0]
            stu_cl = row[4]

    """
            IF cl = 'FF' and 'cum_earn_hrs = 0
                Clean insert
                (By definition, such would not have a prior stu serv rec
                  so the cl is redundant logic.  If they have a stu acad rec,
                  they will need a stu serv rec and there would be nothing
                  to pull from the prior term)
            IF had a room last term...
                (For fall to spring, always copy the last term.  For spring to
                fall ignore)
            IF nothing from previous term 
                clean insert
            If fall term upcoming
                clean insert
            If spring term coming and fall term populated
                Copy fall to spring
                    """

    if target_sess == 'RA' or stu_cl == 'FF':
        print("clean insert - no need to use last term")
        x = insert_ssr(carth_id, target_sess, target_yr, "UN", "000",
                       "", "R", "R")
        print(x)
    else:
        print("search previous term stu_serv_rec")
        """This query will find the prior stu_serv_rec if it exists"""
        last_ssr_sql = last_ssr

        #     # IF WE WANTED TO INCLUDE INCOMING....
        #     # select
        #     # ADM.id, ADM.plan_enr_sess, ADM.plan_enr_yr, ADM.primary_app,
        #     # ADM.enrstat
        #     # from adm_rec ADM
        #     # where
        #     # ADM.plan_enr_sess = "RC"
        #     # and ADM.plan_enr_yr = 2020
        #     # and ADM.enrstat in ('DEPOSIT', 'ADMITTED')
        #     # and ADM.primary_app = 'Y'
        #
        print(last_ssr_sql)
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
                print ("Can use previous term")
                for r in last_ssr:
                    billcode = r[9]
                    bldg = r[5]
                    room = r[6]
                    intdhsg = r[3]
                    rsvstat = r[10]
                    # mealplan = r[7]
                    # parkloc = r[8]

                # x = insert_ssr(carth_id, target_sess, target_yr, bldg,
                #                room, billcode, intdhsg, rsvstat)
                # print(x)
            else:
                print("No prior rec - insert clean")
                # x = insert_ssr(carth_id, target_sess, target_yr,
                #                "UN", "UN", "", "R", "R")
                # print(x)



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

