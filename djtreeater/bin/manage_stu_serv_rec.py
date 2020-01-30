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

    this_yr = 2019
    this_month = 3

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

    print("Target Year = " + target_yr)
    print("Target Sess = " + target_sess)
    #
    # print(last_sess)
    return [last_sess, last_yr, target_sess, target_yr]


def insert_ssr(id, sess, yr, bldg, room, billcode, intendhsg, rsvstat):

    try:
        '''Basic insert sql'''
        q_ins = '''insert into stu_serv_rec
            (id, sess, yr, rsv_stat, 
             intend_hsg, campus, bldg, room,  
             pref_rm_type, roommate_sts, park_location,
             bill_code
            )
        values
            ({0}, "{1}", {2}, "{7}",  
            "{6}", "MAIN", "{3}", "{4}",  
            "", "", "", "{5}")'''.format(id, sess, yr, bldg, room, billcode,
                                         intendhsg, rsvstat )

        print(q_ins)
        connection = get_connection(EARL)
        with connection:
            cur = connection.cursor()
            cur.execute(q_ins)

        connection.commit()

        return 1
    except Exception as e:
        print("Error on insert " + repr(e))
        return 0


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

    if target_sess == 'RA':
        cur_ssr_sql = '''select SAR.id, SAR.sess, SAR.yr, SAR.acst, SAR.cl, 
            SAR.prog, SAR.subprog, SAR.cum_att_hrs, SAR.cum_earn_hrs, SAR.reg_hrs, 
            PER.adm_yr, PER.plan_grad_sess, PER.plan_grad_yr, PER.plan_grad_grp, 
            ST.id, 'MAIN' campus, '' last_bldg, '' last_room,  '' last_billcode,
            '' last_intnd_hsg, '' last_rsv_stat, '' last_park, '' pref_rm_type, 
            '' rmt_stat
            from stu_acad_rec SAR 
            left join stu_stat_rec ST
                on ST.id = SAR.id
                and ST.prog = SAR.prog
                and ST.sess = SAR.sess
                and ST.yr = SAR.yr
            left join prog_enr_rec PER
                on PER.id = SAR.id
            where SAR.sess =  "{0}"
                and SAR.yr = {1}
                and SAR.acst  IN ('GOOD' ,'LOC' ,'PROB' ,'PROC' ,'PROR' ,'READ' 
                        ,'RP', 'SAB' ,'SHAC' ,'SHOC', 'ACPR')
                and SAR.subprog = 'TRAD'  --Don't want PTSM, UWPK, YOP, TRAP
                AND
                (
                --Include if they are either incoming freshmen/transfers
                    (NVL(PER.adm_yr,0)    =  {1}   
                        AND    NVL(PER.adm_sess,'') = "{0}")
                        OR
                    --Have registered for 1 or more credits in the coming session   
                    -- readmits
                    NVL(SAR.reg_hrs, 1) = 1
                        AND
                    --And will not be graduating before spring
                    NVL(TRIM(PER.plan_grad_sess),'')||PER.plan_grad_yr||NVL(PER.plan_grad_grp,'') 
                    NOT IN (
                        TRIM('RC')||TO_CHAR(2019)||TRIM('MAY'),
                        TRIM('RE')||TO_CHAR(2019)||TRIM('SUM'),
                        TRIM('RC')||TO_CHAR(2019)||TRIM('MYST')
                        )
                ) limit 10
                --and SAR.id not in 
                --(select id from stu_serv_rec 
                --where sess =  "{0}" 
                --and yr = {1}) 
        '''.format(target_sess, target_yr)

    else:
        cur_ssr_sql = '''SELECT
            *       --INTO v_cx_id
        FROM
            (
                SELECT
                PER.id, SARSP.sess, SARSP.yr, PER.acst,  PER.cl, PER.prog, 
                PER.subprog,
                SAR.cum_att_hrs, SAR.cum_earn_hrs, SAR.reg_hrs, 
                PER.adm_yr, PER.plan_grad_sess, PER.plan_grad_yr,
                PER.plan_grad_grp, 'MAIN' campus, 
                NVL(SSR.bldg,'') last_bldg, 
                NVL(SSR.room, '') last_room, NVL(SSR.bill_code, '') last_billcode,
                NVL(SSR.intend_hsg, '') last_intnd_hsg,
                NVL(SSR.rsv_stat, '') last_rsv_stat, SSR.park_location,
                '' pref_rm_type, '' rmt_stat
                FROM
                    --Left join - Do we want PER without a SAR record?  Not 
                    --registered
                    prog_enr_rec PER 
                left JOIN stu_acad_rec  SAR    ON    PER.id        =    SAR.id
                        AND    PER.subprog    =    SAR.subprog
                        AND    SAR.yr        = {3}  --    arg_year_fall
                        AND    SAR.sess        = "{2}"    --arg_session_fall
                LEFT JOIN stu_acad_rec    SARSP    ON    PER.id        =    SARSP.id
                        AND    PER.subprog    =    SARSP.subprog
                        AND    SARSP.yr        =  {1}  --    arg_year_spring
                        AND    SARSP.sess        =    "{0}" --arg_session_spring
                LEFT JOIN    stu_serv_rec    SSR    ON    SAR.id    =    SSR.id
                           AND SSR.yr = SAR.yr
                           AND SSR.sess = SAR.sess
                        AND    SSR.yr    = {3}
                        AND SSR.sess = "{2}"
                                                                
                WHERE
                    PER.subprog    =    'TRAD'
                AND
                    --Student should be registered for 1 or more credit hours in the spring
                    NVL(SARSP.reg_hrs, 0)    >=    1
                AND
                    (
                        --Student who attended but withdrew in the fall will 
                        --have 0 registered hours so just look to see if the record exists
                        SAR.reg_hrs IS NOT NULL
                        OR
                        --Student must have either registered for 1 or more 
                        --credit hours in the fall or not attended in the fall (handles readmit scenario)
                        NVL(SAR.reg_hrs, 1)    >=    1
                    )
                AND
                    --exclude those graduating in December or January
                    (
                        NOT (PER.plan_grad_grp    IN    ('DCST','JAN')    
                        AND    PER.plan_grad_sess    =    'RB'    
                        AND    PER.plan_grad_yr    = {1})    --arg_year_spring)
                        OR
                        NOT(PER.plan_grad_grp    =    'DEC'    
                        AND    PER.plan_grad_sess    =    "{0}" --arg_session_fall    
                        AND    PER.plan_grad_yr    =     {1}) --arg_year_fall)
                    )
                UNION
                 --Picks up first time frosh and unclassified transfers
                 --This ignores stu_acad_rec and locates those with
                 --a prog_enr_rec for the spring term or J-term
                 --Should we add if no stu_acad_rec?  Assume they will be
                 --    added as soon as SAR is entered
                SELECT
                    PER.id, PER.adm_sess, PER.adm_yr, PER.acst, PER.cl, PER.prog, 
                    PER.subprog, 
                    SAR.cum_att_hrs, SAR.cum_earn_hrs, SAR.reg_hrs, 
                    PER.adm_yr, PER.plan_grad_sess, PER.plan_grad_yr,
                    PER.plan_grad_grp,
                    'MAIN' campus, 
                    NVL(SSR.bldg,'') last_bldg, 
                    NVL(SSR.room, '') last_room, NVL(SSR.bill_code, '') last_billcode,
                    NVL(SSR.intend_hsg, '') last_intnd_hsg,
                    NVL(SSR.rsv_stat, '') last_rsv_stat, SSR.park_location,
                    '' pref_rm_type, '' rmt_stat
                FROM
                    prog_enr_rec    PER
                    LEFT JOIN        stu_acad_rec    SAR    ON    PER.id        =    SAR.id
                            AND    PER.subprog    =    SAR.subprog
                            AND    SAR.yr        =  {3}  --    arg_year_fall
                            AND    SAR.sess        = 'RB'    --arg_session_fall
                    LEFT JOIN        stu_acad_rec    SARSP    ON    PER.id        =    SARSP.id
                            AND    PER.subprog    =    SARSP.subprog
                            AND    SARSP.yr        =   {3}  --    arg_year_spring
                            AND    SARSP.sess        =    'RC' --arg_session_spring
                                                                
                LEFT JOIN    stu_serv_rec    SSR    ON    SAR.id    =    SSR.id
                           AND SSR.yr = SAR.yr
                           AND SSR.sess = SAR.sess
                           AND    SSR.yr    = {1}
                           AND SSR.sess = "{0}"
                WHERE
                    --PER.adm_yr        =    {1}  --arg_year_spring
                --AND
                    PER.adm_sess    IN    ('RB','RC')  --spring or j-term
                AND
                    PER.subprog        =    'TRAD'
                AND
                    PER.lv_date        IS    NULL
                AND
                    PER.cl            <>    'SP'  --Screens out SPECIAL j-term
            ) 
        --No need to include if they already have the stu_serv_rec
        --where id not in (select id from stu_serv_rec 
        --    where sess = "{0}" 
        --     and yr = )
                
                limit 2
               
        '''.format(target_sess, target_yr, last_sess, last_yr)


    """This query finds those who don't have a stu_serv_rec
    Need only TRAD, not PTSM, UWPk, TRAP (Part-time)"""
    # cur_ssr_sql = '''select SAR.id, SAR.sess, SAR.yr, SAR.acst, SAR.cl,
    #             SAR.cum_att_hrs, SAR.cum_earn_hrs
    #             from stu_acad_rec SAR
    #             where SAR.sess = "{0}"
    #             and SAR.yr = {1}
    #             and SAR.acst  IN ('GOOD' ,'LOC' ,'PROB' ,'PROC' ,'PROR' ,'READ'
    #                 ,'RP', 'SAB' ,'SHAC' ,'SHOC','ACPR')
    #             and SAR.subprog = 'TRAD'
    #             and SAR.id not in (select id from stu_serv_rec
    #             where sess = "{0}"
    #             and yr = {1}) limit 1'''.format(target_sess, target_yr)
    print(cur_ssr_sql)

    connection = get_connection(EARL)
    """ connection closes when exiting the 'with' block """
    with connection:
        data_result = xsql(
            cur_ssr_sql, connection,
            key=settings.INFORMIX_DEBUG
        ).fetchall()
    cur_ssr = list(data_result)
    print(cur_ssr)
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
                # x = insert_ssr(carth_id, target_sess, target_yr, "UN", "000",
                #                "", "R", "R")
                # print(x)
            else:
                print("search previous term stu_serv_rec")
                """This query will find the prior stu_serv_rec if it exists"""
                last_ssr_sql = '''select id, sess, yr, intend_hsg, campus,
                        bldg, room, meal_plan_type, park_location, bill_code,
                        rsv_stat
                        from stu_serv_rec
                        where id = {0}
                        and yr = {1}
                        and sess = "{2}"'''.format(carth_id, last_yr,
                                                   last_sess)

                # IF WE WANTED TO INCLUDE INCOMING....
                # select
                # ADM.id, ADM.plan_enr_sess, ADM.plan_enr_yr, ADM.primary_app,
                # ADM.enrstat
                # from adm_rec ADM
                # where
                # ADM.plan_enr_sess = "RC"
                # and ADM.plan_enr_yr = 2020
                # and ADM.enrstat in ('DEPOSIT', 'ADMITTED')
                # and ADM.primary_app = 'Y'

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

