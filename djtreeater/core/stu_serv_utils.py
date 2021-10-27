from datetime import datetime

def fn_set_term_vars():
    this_month = datetime.now().month

    '''Set false variable for testing'''
    # this_month = 10

    # print("Current Month = " + str(this_month))

    '''for fall term we only write the shell
    for spring term we are passing fall data to spring
    so we can't pass fall data to spring until it exists and is stable
    Fall to Spring should not happen til October or maybe November
    Spring to fall must be running by April 30
    All other time periods will only write the shell'''
    if this_month > 9:
        look_ahead = 'Fall to Spring'
        days = 90
    elif this_month < 5:
        look_ahead = 'Spring to Fall'
        days = 180
    else:
        look_ahead = ''
        days = 0
    # print(days)
    return [days, look_ahead]



