
def get_fall_to_spring(target_sess, target_yr):
    SQL_FALL_TO_SPRING = '''
    SELECT id, prog, subprog, vw_acst, pr_acst, stu_group, 
            reg_hrs, earn_hrs, pr_cl, ar_cl 
            FROM
            (
            -- Get upperclassmen
            SELECT CSV.id, CSV.prog, CSV.subprog, CSV.acst vw_acst, 
            CSV.stu_group, SAR.reg_hrs, SAR.cl ar_cl, SAR.earn_hrs, 
            PER.acst pr_acst, PER.cl pr_cl 
            FROM
            cc_current_students_vw CSV 
            JOIN prog_enr_rec PER 
            ON PER.id = CSV.id AND PER.subprog = 'TRAD' 
            JOIN stu_acad_rec SAR 
            ON CSV.id = SAR.id AND PER.prog = SAR.prog 
                AND PER.subprog = SAR.subprog 
                AND SAR.sess = '{0}'
                AND SAR.yr = {1}
                AND SAR.subprog = 'TRAD' 
                AND SAR.cl != 'SP' 
                --Exclude graduating 
                AND CSV.id NOT IN
                (
                   SELECT id FROM prog_enr_rec PER 
                WHERE 
                (PER.plan_grad_grp in ('DCST', 'JAN')
                AND PER.plan_grad_sess in ('RB') --arg_session_fall    
                AND PER.plan_grad_yr = {1}) --arg_year_fall
                OR
                (PER.plan_grad_grp in ('DEC')
                AND PER.plan_grad_sess in ('RA') --arg_session_fall    
                AND PER.plan_grad_yr = {1}-1) --arg_year_fall
                )
            UNION --INCOMING
            SELECT CSV.id, CSV.prog, CSV.subprog, CSV.acst vw_acst, 
            CSV.stugroup, SAR.reg_hrs, SAR.cl ar_cl, SAR.earn_hrs, 
            PER.acst pr_acst, PER.cl pr_cl 
            FROM cc_incoming_students_vw CSV 
            JOIN prog_enr_rec PER 
            ON PER.id = CSV.id 
                AND    PER.subprog = 'TRAD' 
            JOIN stu_acad_rec SAR 
            ON CSV.id = SAR.id 
                AND PER.prog = SAR.prog 
                AND PER.subprog = SAR.subprog 
                AND
                (
                    (SAR.sess = '{0}'
                        AND SAR.yr = {1} 
                        AND SAR.reg_hrs > 0 
                        AND SAR.subprog = 'TRAD'
                    )
                    OR --Check for previous IN case they took a semester off
                    (SAR.sess = "RC" 
                    AND SAR.yr = {1} 
                    AND SAR.reg_hrs > 0 
                    AND SAR.subprog = 'TRAD'
                    )    
                )
    )
    --Exclude if they already have a stu_serv_rec for the term
    WHERE id NOT IN
    (SELECT id 
    FROM stu_serv_rec 
    WHERE sess = '{0}' AND yr = 2019)
            '''.format(target_sess, target_yr)

    return SQL_FALL_TO_SPRING

def get_spring_to_fall(target_sess, target_yr):

    SQL_SPRING_TO_FALL = '''SELECT
     id, prog, subprog, vw_acst, pr_acst, stu_group, reg_hrs, earn_hrs, 
    pr_cl,  ar_cl
    FROM 
    (
        -- Get upperclassmen
        SELECT CSV.id, CSV.prog, CSV.subprog, CSV.acst vw_acst, 
                CSV.stu_group, SAR.reg_hrs, 
           SAR.cl ar_cl, SAR.earn_hrs,
           PER.acst pr_acst, PER.cl pr_cl
        FROM cc_current_students_vw CSV   --Current students only
        JOIN prog_enr_rec PER             --Enrolled in a program
        ON PER.id = CSV.id
        AND PER.subprog = 'TRAD'

        JOIN stu_acad_rec SAR             --Registered
        ON CSV.id = SAR.id
        AND PER.prog = SAR.prog
        AND PER.subprog = SAR.subprog
        AND SAR.sess = '{0}' and SAR.yr = {1}    --Session to be queried
        AND SAR.subprog = 'TRAD'                 --TRAD only
        AND SAR.cl != 'SP'                       --No JTerm trip only students

        --DO NOT INCLUDE those graduating in the spring (source term)
        AND CSV.id NOT IN
            (
            SELECT id FROM prog_enr_rec PER
            WHERE 
                    (PER.plan_grad_grp IN ('MAY','SUM', 'MYST') 
                    AND PER.plan_grad_sess in ('RC', 'RE')
                    AND PER.plan_grad_yr = {1}) 
            )
        
        UNION
        --INCOMING
        SELECT
        CSV.id, CSV.prog, CSV.subprog, CSV.acst vw_acst, 
                CSV.stugroup, SAR.reg_hrs, 
           SAR.cl ar_cl, SAR.earn_hrs,
           PER.acst pr_acst, PER.cl pr_cl
        FROM cc_incoming_students_vw CSV
        JOIN prog_enr_rec PER
        ON PER.id = CSV.id
        AND PER.subprog = 'TRAD'

        JOIN stu_acad_rec SAR
        ON CSV.id = SAR.id
        AND PER.prog = SAR.prog
        AND 
            (
                (SAR.sess = '{0}' and SAR.yr = {1} 
                        AND SAR.reg_hrs > 0 
                        AND SAR.subprog = 'TRAD')
                OR  --Check for previous in case they took a semester off
                (SAR.sess = "RC" 
                        and SAR.yr = {1} 
                        AND SAR.reg_hrs > 0 
                        AND SAR.subprog = 'TRAD')
            )
)
    --Exclude those already in stu_serv_Rec
        WHERE id NOT IN 
        (SELECT id FROM stu_serv_rec
        WHERE sess = '{0}' and yr = {1}
        )

               
        '''.format(target_sess, target_yr)

    return SQL_SPRING_TO_FALL


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
        # connection = get_connection(EARL)
        # with connection:
        #     cur = connection.cursor()
        #     cur.execute(q_ins)
        #
        # connection.commit()

        return 1
    except Exception as e:
        print("Error on insert " + repr(e))
        return 0

def last_ssr(carth_id, last_yr, last_sess):
    last_ssr_sql = '''select id, sess, yr, intend_hsg, campus,
                    bldg, room, meal_plan_type, park_location, bill_code,
                    rsv_stat
                    from stu_serv_rec
                    where id = {0}
                    and yr = {1}
                    and sess = "{2}"'''.format(carth_id, last_yr,
                                               last_sess)
    return last_ssr_sql