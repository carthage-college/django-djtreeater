ADIRONDACK_QUERY = '''
SELECT --Distinct
    TO_CHAR(IR.id) AS STUDENT_NUMBER,
    TRIM(IR.firstname) AS FIRST_NAME,
    TRIM(IR.middlename) AS MIDDLE_NAME,
    TRIM(IR.lastname) AS LAST_NAME,
    TO_CHAR(birth_date, '%m/%d/%Y') AS DATE_OF_BIRTH,
    PRO.sex AS GENDER,
    '' as IDENTIFIED_GENDER,
    TRIM(ADM.pref_name) AS PREFERRED_NAME,
    'STUDENT' as PERSON_TYPE,
    CASE WHEN (NVL(PRO.priv_code, '') = '') THEN 0
    ELSE -1
    END 
    AS PRIVACY_INDICATOR,
    TRIM(CV.ldap_name) as ADDITIONAL_ID1,
    '' AS ADDITIONAL_ID2,
     CASE 
        WHEN (CL.CL in ('FR', 'FN', 'FF')) THEN 'Freshman'
        --First time frosh
        --should be pulled prior to Aug 1 of their enrollment year
        WHEN (CL.CL = 'SO') THEN 'Sophomore'
        WHEN (CL.CL = 'JR') THEN 'Junior'
        WHEN (CL.CL = 'SR') THEN 'Senior'
        WHEN (CL.CL in ('GR', 'AT')) THEN 'Masters'
        WHEN (CL.CL IN ('ND', 'SP')) THEN ''   -- What do we do with students
            --not yet classified?  Can leave blank
        ELSE ''
        END AS CLASS_STATUS,
    CL.CL as STUDENT_STATUS,   --Want FF, FN, PF, PN, UT here
    PER.plan_grad_yr as CLASS_YEAR,
    TRIM(MAJ1.txt) as MAJOR,
    SAR.reg_hrs AS CREDITS_SEMESTER, --Current term enrollment
    SAR.cum_earn_hrs CREDITS_CUMULATIVE,  --earned credits --Question...
    SAR.cum_gpa AS GPA,
    CASE WHEN len(REPLACE(TRIM(NVL(CELL.phone,'')), '-', '')) = 10
        THEN REPLACE(TRIM(NVL(CELL.phone,'')), '-', '')
        ELSE 
        CASE WHEN len(REPLACE(TRIM(NVL(CELL.line1,'')), '-', '')) = 10
            THEN REPLACE(TRIM(NVL(CELL.line1,'')), '-', '')
            ELSE ''
            END
    END AS MOBILE_PHONE,
    '' as MOBILE_PHONE_CARRIER, --Question...
    0 AS OPT_OUT_OF_TEXT, --Question...
    TRIM(NVL(EML.eml1,'')) AS CAMPUS_EMAIL,
    TRIM(NVL(EML.eml2,'')) AS PERSONAL_EMAIL,
    trim(TO_CHAR(IR.id)||'.jpg') as PHOTO_FILE_NAME,
    '' AS PERM_PO_BOX, '' AS PERM_PO_BOX_COMBO,
    trim(PER.adm_sess)||' '||PER.adm_yr AS ADMIT_TERM,
    -- Term admitted - important for incoming in particular
    CASE WHEN SPORT.descr IS NULL OR TRIM(SPORT.descr) = '' THEN 0
        ELSE -1 
        END AS STUDENT_ATHLETE,
    PRO.ethnic_code AS ETHNICITY,
    IR.aa AS ADDRESS1_TYPE,
    TRIM(IR.addr_line1) as ADDRESS1_STREET_LINE_1,
    TRIM(IR.addr_line2) as ADDRESS1_STREET_LINE_2,
    TRIM(IR.addr_line3) as ADDRESS1_STREET_LINE_3,
    '' as ADDRESS1_STREET_LINE_4,
    TRIM(IR.city) as ADDRESS1_CITY,
    TRIM(IR.st) as ADDRESS1_STATE_NAME,
    TRIM(IR.zip) AS ADDRESS1_ZIP,
    TRIM(IR.ctry) AS ADDRESS1_COUNTRY,
    CASE WHEN len(REPLACE(TRIM(NVL(IR.phone,'')), '-', '')) = 10
        THEN REPLACE(TRIM(NVL(IR.phone,'')), '-', '')
        ELSE ''
        END AS ADDRESS1_PHONE,

    --Is there an off campus local address?
    CASE WHEN length(trim(LOC.line1)) > 0
        THEN 'LOCAL'
    ELSE ''
    END AS ADDRESS2_TYPE,
    TRIM(LOC.line1) as ADDRESS2_STREET_LINE_1,
    TRIM(LOC.line2) as ADDRESS2_STREET_LINE_2,
    TRIM(LOC.line3) as ADDRESS2_STREET_LINE_3,
    '' as ADDRESS2_STREET_LINE_4,
    TRIM(LOC.city) as ADDRESS2_CITY,
    TRIM(LOC.st) as ADDRESS2_STATE_NAME,
    TRIM(LOC.zip) AS ADDRESS2_ZIP,
    TRIM(LOC.ctry) AS ADDRESS2_COUNTRY,
    CASE WHEN len(REPLACE(TRIM(NVL(LOC.phone,'')), '-', '')) = 10
        THEN REPLACE(TRIM(NVL(LOC.phone,'')), '-', '')
        ELSE ''
    END AS ADDRESS2_PHONE,

    ''  AS ADDRESS3_TYPE,
    TRIM('') as ADDRESS3_STREET_LINE_1,
    TRIM('') as ADDRESS3_STREET_LINE_2,
    TRIM('') as ADDRESS3_STREET_LINE_3,
    '' as ADDRESS3_STREET_LINE_4,
    TRIM('') as ADDRESS3_CITY,
    TRIM('') as ADDRESS3_STATE_NAME,
    TRIM('') AS ADDRESS3_ZIP,
    TRIM('') AS ADDRESS3_COUNTRY,
    '' AS ADDRESS3_PHONE,

    'EMERGENCY' AS CONTACT1_TYPE,
    trim(EMER.line1) as CONTACT1_NAME,
    '' AS CONTACT1_RELATIONSHIP,
    EMER.phone AS CONTACT1_HOME_PHONE,
    '' AS CONTACT1_WORK_PHONE,
    '' AS CONTACT1_MOBILE_PHONE,
    '' AS CONTACT1_EMAIL,
    '' AS CONTACT1_STREET,
    '' AS CONTACT1_STREET2,
    '' AS CONTACT1_CITY,
    '' AS CONTACT1_STATE,
    '' AS CONTACT1_ZIP,
    '' AS CONTACT1_COUNTRY,

    '' AS CONTACT2_TYPE,
    '' as CONTACT2_NAME,
    '' AS CONTACT2_RELATIONSHIP,
    '' AS CONTACT2_HOME_PHONE,
    '' AS CONTACT2_WORK_PHONE,
    '' AS CONTACT2_MOBILE_PHONE,
    '' AS CONTACT2_EMAIL,
    '' AS CONTACT2_STREET,
    '' AS CONTACT2_STREET2,
    '' AS CONTACT2_CITY,
    '' AS CONTACT2_STATE,
    '' AS CONTACT2_ZIP,
    '' AS CONTACT2_COUNTRY,

     '' AS CONTACT3_TYPE,
    '' as CONTACT3_NAME,
    '' AS CONTACT3_RELATIONSHIP,
    '' AS CONTACT3_HOME_PHONE,
    '' AS CONTACT3_WORK_PHONE,
    '' AS CONTACT3_MOBILE_PHONE,
    '' AS CONTACT3_EMAIL,
    '' AS CONTACT3_STREET,
    '' AS CONTACT3_STREET2,
    '' AS CONTACT3_CITY,
    '' AS CONTACT3_STATE,
    '' AS CONTACT3_ZIP,
    '' AS CONTACT3_COUNTRY,

    TRIM(TRIM(TRM.sess)||' '||TRIM(TO_CHAR(TRM.yr))) as TERM,

    --Custom fields
     PRO.race as RACECODE,
     SPORT.DESCR AS SPORT,
     GREEK.ORG AS GREEK_LIFE
-- these will probably be manually populated
--    '' as preferred_pronoun,
--    '' as Service_Emotional_Support_Animal,

FROM
    (
    select * from
        (SELECT unique PV.id, PR.prog, PR.subprog, PR.major1 as major,
            PV.student,
            PR.acst, PR.cl, PR.major1, PR.plan_grad_yr, PR.adm_sess, PR.adm_yr,
            row_number() over ( partition BY PR.id
            ORDER BY 
                CASE when PR.prog = 'GRAD' then 1
                    when PR.prog = 'UNDG' then 2
                    when PR.prog = 'PRDV' then 3
                    WHEN PR.PROG = 'ACT' THEN 4
                    when PR.prog = 'PARA' then 5
                    else 9 end )
                    as row_num
        from provisioning_vw PV
        LEFT JOIN prog_enr_rec PR
                ON PV.id = PR.id
        WHERE PV.student IN ('prog', 'stu', 'reg_clear')
            AND PR.acst IN ('GOOD' ,'LOC' ,'PROB' ,'PROC' , 'PROR' ,'READ' ,
            'RP','SAB','SHAC' ,'SHOC', 'GRAD', 'ACPR')
            AND (PR.subprog NOT IN ('KUSD', 'UWPK', 'YOP', 'ENRM'))
            AND (PR.CL != 'UP')
            AND (PR.lv_date IS NULL)
            AND (PR.deg_grant_date IS NULL)
          ) rnk_prog
    WHERE row_num = 1
    ) PER

    INNER JOIN    id_rec        IR    ON    PER.id            =    IR.id

    LEFT JOIN 
        (  SELECT a1.id id1, a1.line1 eml1, a2.id id2, a2.eml2
            FROM aa_rec a1
            LEFT JOIN
               (
                SELECT id, aa, line1 eml2, beg_date, end_date 
                FROM aa_rec 
                WHERE aa = 'EML2'            
                AND  beg_date < TODAY
                AND NVL(end_date, TODAY) >= TODAY     
              ) a2 
            ON a2.id = a1.id 
            WHERE 
            (
                a1.aa = 'EML1'
                AND    a1.beg_date < TODAY
                AND NVL(a1.end_date, TODAY) >= TODAY
            )
        ) EML on EML.id1 = PER.id 

    LEFT JOIN
        (SELECT id, line1, line2, line3, city, st, zip, ctry, phone
        FROM aa_rec
        WHERE aa = 'LOC'
        AND (end_date IS NULL OR end_date >= TODAY)) LOC
        ON LOC.id = PER.id

    LEFT JOIN        (
        SELECT id.id,
                replace(replace(replace(replace(replace(replace(replace(
                multiset(
            SELECT DISTINCT trim(a) from
                (SELECT invl_table.txt a
                FROM involve_rec
                  JOIN invl_table
                ON invl_table.invl=involve_rec.invl
                WHERE id=id.id
                AND invl_table.sanc_sport = 'Y'
                --Check this.  Do we need to use the term dates?
                and involve_rec.end_date > TODAY
                ORDER BY invl_table.txt)
                )::lvarchar,'MULTISET{'), 'ROW'), '}'),"')",''),
                "('",''), ',',';'), "''","'")
                DESCR
            FROM id_rec id
            ) SPORT ON SPORT.id = PER.id


     LEFT JOIN (
                select distinct id, org from involve_rec
            where ctgry = 'GREEK'
            and beg_date > TODAY - 180
            and (end_date > TODAY or end_date is null)
            ) GREEK
            on GREEK.id = PER.id

    JOIN (    select distinct id, sess, yr
        from stu_serv_rec
        where sess in ('RA','RC')
            AND sess||YR in (select sess||yr from acad_cal_rec
                where end_date > TODAY -1
            and sess in ('RA','RC'))
            ) 
            TRM ON TRM.id = PER.id

    INNER JOIN    cvid_rec CV    ON    PER.id = CV.cx_id
    INNER JOIN    cl_table CL    ON    PER.cl = CL.cl
    LEFT JOIN major_table MAJ1    ON    PER.major1 = MAJ1.major
    inner JOIN adm_rec ADM    ON    PER.id = ADM.id
        AND ADM.prog = PER.prog
        AND    ADM.primary_app    =    'Y'
    INNER JOIN    profile_rec    PRO    ON    PER.id = PRO.id
    LEFT JOIN 
        (SELECT a.id ID, a.aa aa, a.line1 line1,
            a.phone phone, a.beg_date beg_date
        FROM aa_rec a
        INNER JOIN
            (
            SELECT id, MAX(beg_date) beg_date
            FROM aa_rec
            WHERE aa = 'CELL'
            GROUP BY id
            ) b 
            ON a.id = b.id AND a.beg_date = b.beg_date
            AND a.aa = 'CELL'
        ) CELL
        ON CELL.ID = PER.ID

    LEFT JOIN
        (SELECT id, gpa, mflag
        FROM degaudgpa_rec
        WHERE mflag = 'MAJOR1' AND gpa > 0
        ) DGR
        ON     DGR.id = PER.ID

    LEFT JOIN
        (SELECT distinct sr.prog, sr.id, sr.subprog,
            sr.cum_gpa, sr.yr, sr.subprog, sr.earn_hrs, sr.cum_earn_hrs,
            sr.reg_hrs
        FROM stu_acad_rec sr, cursessyr_vw cv
        WHERE sr.sess = cv.sess
        AND sr.prog = cv.prog
        AND sr.yr = cv.yr) SAR
          ON SAR.id = PER.id
          AND SAR.prog = PER.prog

        --Don't bother with ICE1 or ICE2, little data...
    LEFT JOIN
        (SELECT id, line1, line2, line3, city, st, zip, ctry, phone
        FROM aa_rec
        WHERE aa = 'ICE'
        AND (end_date IS NULL OR end_date >= TODAY)) EMER
        ON EMER.id = PER.id

'''

Q_GET_TERM = '''select distinct 
                  trim(trim(sess)||' '||trim(TO_CHAR(yr))) session
                  from acad_cal_rec
                  where sess in ('RA','RC')
                  and subsess = ''
                  and prog = 'UNDG'
                  and trim(sess)||TO_CHAR(yr) = 
                  CASE
                      -- ACYR 1920 After April 20 
                      WHEN TODAY >= '04/20/'||YEAR(TODAY)
                          AND TODAY < '12/30/'||YEAR(TODAY)
                          AND LEFT(ACYR,2) = RIGHT(TO_CHAR(YEAR(TODAY)),2)
                          THEN
                              'RA'||YEAR(TODAY)
                      --ACYR 2021 after Jan 1 until April 20
                      WHEN TODAY >= '01/01/'||YEAR(TODAY)
                          AND TODAY < '04/20/'||YEAR(TODAY)
                          AND LEFT(ACYR,2) = RIGHT(TO_CHAR(YEAR(TODAY)),2)
                          THEN
                              'RC'||YEAR(TODAY)
                      END
                   '''
