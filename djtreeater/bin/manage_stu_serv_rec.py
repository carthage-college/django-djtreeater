# -*- coding: utf-8 -*-

import os, sys
from datetime import datetime
# env
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "djtreeater.settings.shell")

import django
django.setup()

from django.conf import settings
from djimix.core.utils import get_connection, xsql
from djtreeater.sql.stu_serv_rec import get_spring_to_fall, \
    get_fall_to_spring, insert_ssr, get_last_ssr, find_future_terms
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
    if database == 'cars':
        EARL = settings.INFORMIX_ODBC
    elif database == 'train':
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
    
    Incoming first time students will get a basic entry w/o bldg, room, 
    parking,bill code, meal plan  
    Should start looking Nov 1 for upcoming spring term and March 15 
    for upcoming fall term
    
    Returning students will get a basic minimal entry for the fall term
    
    For the spring term, starting March 15 we will want to copy the info 
    from the fall term with the possible exception of the parking entry.  
    That gets billed only for the year, not for the second semester.
    
    """
    # print(EARL)

    '''determine how many days to look ahead'''
    this_month = datetime.now().month
    if this_month > 9:
        days = 90
    elif this_month < 5:
        days = 180
    else:
        days = 45
    print(days)

    '''New process
    1. Find upcoming terms
    2. loop through those terms and update as needed
    2a.  find students enrolled in each term
    2b   find if they exist in stu_serv_rec
    2c add if needed'''

    '''get list of terms to search'''
    future_terms = find_future_terms(days)
    # print("Future Terms SQL = " + str(future_terms))

    connection = get_connection(EARL)
    """ connection closes when exiting the 'with' block """
    with connection:
        data_result = xsql(
            future_terms, connection,
            key=settings.INFORMIX_DEBUG
        ).fetchall()
    fut_terms = list(data_result)
    print("Future terms = " + str(fut_terms))

    '''Loop through upcoming terms'''
    for row in fut_terms:
        # sess = row[0]
        # yr = row[1]
        sess = 'RC'
        yr = 2022
        # season = row[2].strip()
        season = "FALL"
        cur_ssr_sql = ''
        print(sess)
        print(yr)
        print(season)
        '''Find students missing a student service record for the terms
        in question'''
        # season = ''
        if season == 'FALL':
            cur_ssr_sql = get_spring_to_fall(sess, yr)
        elif season == 'SPRING':
            cur_ssr_sql = get_fall_to_spring(sess, yr)
        else:
            cur_ssr_sql = get_spring_to_fall('', 0)

        # print(cur_ssr_sql)

        connection = get_connection(EARL)
        """ connection closes when exiting the 'with' block """
        with connection:
            data_result = xsql(cur_ssr_sql, connection,
                               key=settings.INFORMIX_DEBUG).fetchall()
        if data_result:
            # print(data_result)
            cur_ssr = list(data_result)
            # print(cur_ssr)
            if len(cur_ssr) != 0:
                for row in cur_ssr:
                    print('----------------')
                    print("Stu Serv Rec needed for " + str(row[0]) + ' for '
                          + sess + ' ' + str(yr))
                    print(row)
                    carth_id = row[0]
                    # stu_cl = row[9]
                    # earn_hrs = row[6]

                    '''---------------------------------'''
                    """Fall term is always a clean insert - no parking info, 
                        those will come later via ???"""
                    '''---------------------------------'''
                    if season == 'SPRING':
                        print("clean insert - no need to use last term")
                        insSql = insert_ssr(carth_id, sess, yr, "",
                                            "",
                                            "", "R", "R",'','', EARL)
                        print(insSql)
                        # cur = connection.cursor()
                        # cur.execute(x)
                        # connection.commit()
                        exit()
                    elif season == 'FALL':
                        print("search fall term stu_serv_rec")
                        '''---------------------------------'''
                        """This query will find the prior stu_serv_rec if it 
                            exists"""
                        '''---------------------------------'''
                        last_ssr_sql = get_last_ssr(carth_id, yr - 1, 'RA')
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
                                print ("Can use previous term")
                                for r in last_ssr:
                                    billcode = r[9]
                                    bldg = r[5]
                                    room = r[6]
                                    intdhsg = r[3]
                                    rsvstat = r[10]
                                    mealplan = r[7]
                                    print(mealplan)
                                    parkloc = r[8]
                                    # parkloc = '37.1'

                                    print(parkloc)
                                    # print(parkloc[-2:])
                                    if not parkloc:
                                        print("No Parking data")
                                        parkloc = ''
                                    elif parkloc[-2:] == '.5' or parkloc[
                                                               -2:] == '.4':
                                        print("Spring payment")
                                        parkloc = ''
                                    else:
                                        print("Full year payment")

                                """Here I need something to decipher the 
                                existing entry.for parking
                                If we do this, then if the park_location field
                                is the key.  If it ends in .1 or .9, then the
                                fall fee covers the year and it should carry
                                over to the spring.  Spring values do NOT
                                move to the fall term"""

                                print ("Insert " + str(carth_id) + ', ' + sess
                                       + ', ' + str(yr) + ', ' + bldg
                                       + ', ' + room + ', ' + billcode
                                       + ', ' + intdhsg + ', ' + rsvstat
                                       + ', ' + parkloc)

                                x = insert_ssr(carth_id, sess,
                                yr, bldg, room, billcode, intdhsg,
                                          rsvstat, '', parkloc,  EARL)
                                print(x)
                                connection = get_connection(EARL)
                                print("insert record using last term")
                                # cur = connection.cursor()
                                # cur.execute(x)
                                # connection.commit()
                                # exit()
                            else:
                                print("No prior term - insert clean")
                                x = insert_ssr(carth_id, sess,
                                yr, "UN", "UN", "", "R", "R",'', '', EARL)
                                print(x)
                                with connection:
                                    print("clean insert")
                                    # cur = connection.cursor()
                                    # cur.execute(x)
                                    # connection.commit()
                                    # exit
                                    # ()
                    else:
                        pass
            else:
                print("Nothing to do")

        else:
            print('No Data')

        print('---------------')





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

