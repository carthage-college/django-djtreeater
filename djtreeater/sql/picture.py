# from sqlalchemy import text

PICTURE_ID_QUERY = '''
SELECT Distinct 
    TO_CHAR(IR.id) AS STUDENT_ID  --, 
FROM
    (SELECT unique PV.id, PV.program, PV.student,
        PR.acst, PR.cl
        from provisioning_vw PV
        LEFT JOIN prog_enr_rec PR
            ON PV.id = PR.id
        WHERE PV.student IN ('prog', 'stu', 'reg_clear')
            AND PR.acst IN ('GOOD' ,'LOC' ,'PROB' ,'PROC' , 'PROR' ,'READ' ,
        'RP','SAB','SHAC' ,'SHOC', 'GRAD')
            AND (PR.subprog NOT IN ('KUSD', 'UWPK', 'YOP', 'ENRM'))
            AND (PR.CL != 'UP')
            AND (PR.lv_date IS NULL)
            AND (PR.deg_grant_date IS NULL)
    ) PER
    INNER JOIN id_rec IR ON PER.id = IR.id
    INNER JOIN cl_table CL ON PER.cl = CL.cl
    INNER JOIN profile_rec PRO ON PER.id = PRO.id
where PRO.priv_code != 'FERP'
'''
# LIMIT 10

LENEL_PICTURE_QUERY = """
    SELECT
        MMOBJS.LNL_BLOB as photo
    FROM
        EMP left join MMOBJS on MMOBJS.EMPID = EMP.ID
    WHERE
        mmobjs.object = 1
    AND
        mmobjs.type = 0
    AND
       emp.ssno = {0}
"""

#        emp.ssno = '#LMS_students.host_id#'
