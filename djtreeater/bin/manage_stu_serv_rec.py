# -*- coding: utf-8 -*-

import os, sys
from datetime import datetime
# env
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "djtreeater.settings.shell")

import django
django.setup()

from django.conf import settings

import argparse
import logging

logger = logging.getLogger('djtreeater')

'''
Shell script...
'''

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

def main():
    '''
    main function
    '''

    if test:
        print("this is a test")
        logger.debug("debug = {}".format(test))
    else:
        print("this is not a test")

        # set global variable
        global EARL
        # determines which database is being called from the command line
        if database == 'cars':
            EARL = settings.INFORMIX_ODBC_TRAIN
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

For the spring term, we will want to copy the info from the fall term with 
the possible exception of the parking entry.  That gets billed only for the 
year, not for the second semester.
"""

'''To locate students who need an entry'''

# this_yr = datetime.now().year
# this_month = datetime.now().month

this_yr = 2020
this_month = 9

print("Current Month = " + str(this_month))
print("Current Year = " + str(this_yr))

if this_month > 11 or this_month < 4:
    target_sess = 'RC'
    last_sess = 'RA'
else:
    target_sess = 'RA'
    last_sess = 'RC'

if this_month > 11 :
    target_yr = str(this_yr + 1)
else:
    target_yr = str(this_yr)

print("Target Year = " + target_yr)
print("Target Sess = " + target_sess)

print(last_sess)



sql = "select SAR.id, SAR.sess, SAR.yr, SAR.acst, SAR.cl, SAR.cum_att_hrs, " \
      "SAR.cum_earn_hrs " \
      "from stu_acad_rec SAR " \
      " where SAR.sess = '" + target_sess + "'" + \
      " and SAR.yr = " + target_yr + \
      " and SAR.id not in " \
      "(select id from stu_serv_rec " \
      " where sess = '" + target_sess  + "'" +\
      " and yr = " + target_yr

print(sql)



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


'''Basic insert sql'''
# insert into cx_sandbox:stu_serv_rec
#        (sess, yr, id, stat, intend_hsg, offcampus_res_appr,
#     rsv_stat, campus, bldg, room, suite, no_per_room,
# res_asst, meal_plan_type, meal_plan_wvd, asb_fee_wvd,
# hlth_ins_wvd, late_reg,emer_phone, emer_phone_ext, emer_ctc_name,
# campus_box, park_prmt_no, park_prmt_exp_date, park_location,
# veh_type, veh_license, veh_lic_st, veh_year,
# veh_make, veh_model, pref_rm_type, roommate_sts,
# crm_add_date, crm_upd_date	)
# 	values
# ("RA", "2020", 123456, "I", "R", "N", "R", "MAIN", "", "", "", "2",
# "N", "F", "N", "N", "N", "N", "", "", "", "", "", "", "", "", "", "", "",
# "", "", "", "", "", "" )



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

