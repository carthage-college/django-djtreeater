
def fn_set_term_vars():
    # this_yr = datetime.now().year
    # this_month = datetime.now().month

    this_yr = 2019
    this_month = 5

    # print("Current Month = " + str(this_month))
    # print("Current Year = " + str(this_yr))

    if this_month > 10 or this_month < 4:
        target_sess = 'RC'
        last_sess = 'RA'
    else:
        target_sess = 'RA'
        last_sess = 'RC'

    if this_month > 11:
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
