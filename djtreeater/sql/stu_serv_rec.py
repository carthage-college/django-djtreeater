
from djimix.core.utils import get_connection, xsql


def get_fall_to_spring(target_sess, target_yr):
    """FIND ALL CURRENT STUDENTS WHO ARE ENROLLED OR REGISTERED FOR THE
        UPCOMING SPRING TERM OR WERE REGISTERED IN THE FALL ???
        AND EXCLUDE GRADUATING STUDENTS
        AND STUDENTS WHO ALREADY HAVE THE STU_SERV_REC FOR THE TERM"""
    SQL_FALL_TO_SPRING = '''SELECT
            id, prog, subprog, acst, 
            reg_hrs, ar_cl, earn_hrs, sess, yr, cl
         FROM
             (
             SELECT
            PER.id, PER.prog, PER.subprog, PER.acst, 
            SAR.reg_hrs, SAR.cl ar_cl, SAR.earn_hrs, SAR.sess, SAR.yr, PER.cl
                FROM
                    prog_enr_rec    PER    
                LEFT JOIN        stu_acad_rec    SAR    
                    ON    PER.id        =    SAR.id
                    AND    PER.subprog    =    SAR.subprog
                    AND    SAR.yr        =  {1}  --    arg_year_spring
                    AND    SAR.sess        =    '{0}' --arg_session_spring
     
                 WHERE
                    PER.subprog    =    'TRAD'
                AND
                    --Student should be registered for 1 or more credit hours
                    -- in the spring
                    NVL(SAR.reg_hrs, 0)    >=    1
                AND
                    --Not graduating in December or January
                    (
                        NOT (PER.plan_grad_grp  IN  ('DCST','JAN')    
                        AND    PER.plan_grad_sess   =    'RB'    
                        AND    PER.plan_grad_yr  = {1} )  --arg_year_spring)
                        OR
                        NOT(PER.plan_grad_grp  =  'DEC'    
                        AND    PER.plan_grad_sess  =  'RA' --arg_session_fall    
                        AND    PER.plan_grad_yr  =  {1} - 1 ) --arg_year_fall)
                    )
                UNION
                SELECT
                    PER.id, 
                    PER.prog, PER.subprog, PER.acst, 
                     0 reg_hrs, PER.cl, 0 earn_hrs, PER.adm_sess sess, 
                     PER.adm_yr yr, PER.cl
                FROM
                    prog_enr_rec    PER
                WHERE
                    PER.adm_yr        =    {1}   --arg_year_spring
                AND
                    PER.adm_sess    IN    ('RB','RC')
                AND
                    PER.subprog        =    'TRAD'
                AND
                    PER.lv_date        IS    NULL
                AND
                    PER.cl            <>    'SP'
            )
        WHERE id NOT IN
    (SELECT id 
    FROM stu_serv_rec 
    WHERE sess = '{0}' AND yr = {1} ) 

            '''.format(target_sess, target_yr)

    return SQL_FALL_TO_SPRING

def get_spring_to_fall(target_sess, target_yr):
    """FIND ALL CURRENT STUDENTS WHO ARE ENROLLED OR REGISTERED FOR THE
         UPCOMING FALL TERM OR WERE REGISTERED IN THE SPRING ???
         AND EXCLUDE GRADUATING STUDENTS
         AND STUDENTS WHO ALREADY HAVE THE STU_SERV_REC FOR THE TERM"""

    SQL_SPRING_TO_FALL = '''
    SELECT PER.id, SAR.prog, SAR.subprog, SAR.acst, 
            SAR.reg_hrs, SAR.cl ar_cl, SAR.earn_hrs, SAR.sess, SAR.yr, PER.cl
              --INTO v_cx_id
        FROM
            prog_enr_rec    PER    
        LEFT JOIN    stu_acad_rec    SAR    
            ON    PER.id        =    SAR.id
            AND    SAR.yr        =    {1}  --arg_year
            AND    SAR.sess    =    '{0}'
        WHERE
            --Get students with an active academic standing
            PER.acst    IN    ('ACPR','CIC','GOOD','LOC','PROB','PROC','PROR',
                'READ','SAB','SHAC','SHOC')
        AND
            --Who are traditional students
            PER.subprog    =    'TRAD'
        AND
            (
                --Are either incoming freshmen/transfers
                (NVL(PER.adm_yr,0)    =  {1}   --    arg_year    
                AND    NVL(PER.adm_sess,'')    =    '{0}')
                OR
                --Have registered for 1 or more credits in the coming session
                NVL(SAR.reg_hrs, 0)            >=    1
            )
        AND
            --And will not be graduating before spring
            NVL(PER.plan_grad_sess,'') || NVL(PER.plan_grad_yr,'') || NVL(PER.plan_grad_grp,'')    
            NOT IN    ('RC' || {1} || 'MAY', 'RE' || {1} || 'SUM', 'RC' || {1} || 'MYST')
            
       -- AND PER.id = 1409500 

        AND PER.id NOT IN
    (SELECT id 
    FROM stu_serv_rec 
    WHERE sess = '{0}' AND yr = {1}) 
   --limit 100
        '''.format(target_sess, target_yr)

    return SQL_SPRING_TO_FALL


def insert_ssr(id, sess, yr, bldg, room, billcode, intendhsg, rsvstat, EARL):

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

        # print(q_ins)
        connection = get_connection(EARL)
        with connection:
            cur = connection.cursor()
            cur.execute(q_ins)

        connection.commit()

        return 1
    except Exception as e:
        print("Error on insert " + repr(e))
        return 0

def get_last_ssr(carth_id, last_yr, last_sess):
    last_ssr_sql = '''select id, sess, yr, intend_hsg, campus,
                    bldg, room, meal_plan_type, park_location, bill_code,
                    rsv_stat
                    from stu_serv_rec
                    where id = {0}
                    and yr = {1}
                    and sess = "{2}"'''.format(carth_id, last_yr,
                                               last_sess)
    return last_ssr_sql