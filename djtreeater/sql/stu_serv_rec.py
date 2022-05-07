
from djimix.core.utils import get_connection, xsql


def get_fall_to_spring(sess, yr):
    """FIND ALL CURRENT STUDENTS WHO ARE ENROLLED OR REGISTERED FOR THE
        UPCOMING SPRING TERM OR WERE REGISTERED IN THE FALL ???
        AND EXCLUDE GRADUATING STUDENTS
        AND STUDENTS WHO ALREADY HAVE THE STU_SERV_REC FOR THE TERM"""

    SQL_FALL_TO_SPRING = '''
        SELECT
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
                     and PER.subprog in ('TRAD', 'TRAP', 'MED', 'MM', 'BDI')
                     and SAR.sess = '{0}'
                     and SAR.yr = {1}
                JOIN acad_cal_rec ACR 
                    on ACR.sess= SAR.sess
                    and ACR.yr = SAR.yr
                    and ACR.end_date > TODAY
                    and ACR.prog = SAR.prog
                    and ACR.prog in ('UNDG', 'GRAD')
                    and ACR.subsess = ''
                    and ACR.sess not in ('KA', 'KC', 'KE', 'KS', 'QA', 'QB', 
                    'YA', 'YC', 'YE', '', 'FOOT', 'PREV', 'HEAD', 'TRAN', 
                    'PA', 'PC', 'PE')
                WHERE
                    --Get students with an active academic standing
                      PER.acst    IN    ('ACPR','CIC','GOOD','LOC','PROB','PROC','PROR',
                        'READ','SAB','SHAC','SHOC')
                AND
                    --Who are traditional students
                       PER.prog    in  ('UNDG', 'GRAD')   
                AND
                    --Student should be registered for 1 or more credit hours
                    -- in the spring
                    NVL(SAR.reg_hrs, 0)    >=    1
                AND
                    --Not graduating in December or January
                     (
                        NOT (PER.plan_grad_grp  IN  ('DCST','JAN')    
                        AND    PER.plan_grad_sess   =    'RB'    
                        AND    PER.plan_grad_yr  =  YEAR(TODAY) + 1)  --arg_year_spring
                        OR
                        NOT (PER.plan_grad_grp  =  'DEC'    
                        AND    PER.plan_grad_sess  =  'RA' --arg_session_fall    
                        AND    PER.plan_grad_yr  =  YEAR(TODAY)) --arg_year_fall
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
                    PER.adm_yr        =    YEAR(TODAY) + 1   --arg_year_spring
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
            WHERE sess = '{0}' and yr = {1})
            limit 10
            --     ) 
    '''.format(sess, yr)


    return SQL_FALL_TO_SPRING

def get_spring_to_fall(sess, yr):
    """FIND ALL CURRENT STUDENTS WHO ARE ENROLLED OR REGISTERED FOR THE
         UPCOMING FALL TERM OR WERE REGISTERED IN THE SPRING ???
         AND EXCLUDE GRADUATING STUDENTS
         AND STUDENTS WHO ALREADY HAVE THE STU_SERV_REC FOR THE TERM"""
    SQL_SPRING_TO_FALL = '''
     SELECT  distinct PER.id, SAR.prog, SAR.subprog, SAR.acst, 
          SAR.reg_hrs, SAR.cl ar_cl, SAR.earn_hrs, SAR.sess, SAR.yr, PER.cl
              --INTO v_cx_id
        FROM
            prog_enr_rec    PER    
        JOIN    stu_acad_rec    SAR    
            ON    PER.id        =    SAR.id
            and PER.prog = SAR.prog
            AND    SAR.yr        =    {1}  --arg_year
            AND    SAR.sess    =    '{0}'
            and PER.subprog in ('TRAD', 'TRAP', 'MED', 'MM', 'BDI')
        JOIN acad_cal_rec ACR 
            on ACR.sess= SAR.sess
            and ACR.yr = SAR.yr
            and ACR.end_date > TODAY
            and ACR.prog = SAR.prog
            and ACR.prog in ('UNDG', 'GRAD')
            and ACR.subsess = ''
            and ACR.sess not in ('KA', 'KC', 'KE', 'KS', 'QA', 'QB', 
                    'YA', 'YC', 'YE', '', 'FOOT', 'PREV', 'HEAD', 'TRAN', 
                    'PA', 'PC', 'PE')
        WHERE
            --Get students with an active academic standing
            PER.acst    IN    ('ACPR','CIC','GOOD','LOC','PROB','PROC','PROR',
                'READ','SAB','SHAC','SHOC')
        AND
            --Who are traditional students
           PER.prog    in  ('UNDG', 'GRAD')
        AND
            (
                --Are either incoming freshmen/transfers
                (NVL(PER.adm_yr,0)    =  YEAR(TODAY)   --    arg_year    
                AND    NVL(PER.adm_sess,'')    =    'RA')
                OR
                --Have registered for 1 or more credits in the coming session
                NVL(SAR.reg_hrs, 0)            >=    1
            )
        AND
            --And will not be graduating before spring
            NVL(PER.plan_grad_sess,'') || NVL(PER.plan_grad_yr,'') || NVL(PER.plan_grad_grp,'')    
            NOT IN    ('RC' || YEAR(TODAY) || 'MAY', 'RE' || YEAR(TODAY) || 'SUM', 'RC' || YEAR(TODAY) || 'MYST')
        AND PER.id NOT IN
            (SELECT id
            FROM stu_serv_rec
            WHERE sess = '{0}' and yr = {1})
            --limit 10
    '''.format(sess, yr)



    return SQL_SPRING_TO_FALL


def insert_ssr(id, sess, yr, bldg, room, billcode, intendhsg, rsvstat, meal,
               parking, EARL):

    try:
        '''Basic insert sql'''
        q_ins = '''insert into stu_serv_rec
            (id, sess, yr, rsv_stat, 
             intend_hsg, campus, bldg, room,  
             pref_rm_type, roommate_sts, park_location,
             bill_code, meal_plan_type
            )
        values
            ({0}, "{1}", {2}, "{7}",  
            "{6}", "MAIN", "{3}", "{4}",  
            "", "", "{8}", "{5}", "")'''.format(id, sess, yr, bldg, room,
                            billcode, intendhsg, rsvstat, parking )

        return q_ins
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

def find_future_terms(days):

    find_future_term_sql = '''
        select distinct trim(sess), yr,
            CASE WHEN MONTH(beg_date) < 5 THEN 'SPRING'
                 WHEN MONTH(beg_date) < 8 THEN 'SUMMER'
            ELSE 'FALL'
            END as season
        from acad_cal_rec
        where beg_date < TODAY + {0}
        and end_date > TODAY
        and subsess = ''
        and sess not in ('KA', 'KC', 'KE', 'KS', 'QA', 'QB', 'YA', 'YC', 
        'YE', '', 'FOOT', 'PREV', 'HEAD', 'TRAN', 'PA', 'PC', 
        'PE')'''.format(days)

    return find_future_term_sql