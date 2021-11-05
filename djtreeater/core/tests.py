# -*- coding: utf-8 -*-
import os
import shutil
import sys
import time
import datetime as dt
from datetime import datetime
import codecs
import hashlib
import json
import requests
import csv
import argparse
import logging
import django
import string

# from string import maketrans
# ________________
# Note to self, keep this here
# django settings for shell environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "djtreeater.settings.shell")
django.setup()
# ________________

from django.conf import settings
from djtreeater.core.utilities import fn_write_error, \
    fn_write_billing_header, fn_get_utcts, \
    fn_encode_rows_to_utf8, fn_mark_room_posted, \
    fn_mark_bill_exported, fn_check_cx_records, \
    fn_sendmailfees, fn_sendmailfees_all_trms, \
    fn_lookup_terms, fn_lookup_accounts
from djtreeater.core.lookuplist import fn_lookuplist

from djimix.core.utils import get_connection, xsql
from datetime import timedelta
from djtreeater.core.utilities import fn_lookup_terms
# from django.test import TestCase

# informix environment
os.environ['INFORMIXSERVER'] = settings.INFORMIXSERVER
os.environ['DBSERVERNAME'] = settings.DBSERVERNAME
os.environ['INFORMIXDIR'] = settings.INFORMIXDIR
os.environ['ODBCINI'] = settings.ODBCINI
os.environ['ONCONFIG'] = settings.ONCONFIG
os.environ['INFORMIXSQLHOSTS'] = settings.INFORMIXSQLHOSTS
os.environ['LD_LIBRARY_PATH'] = settings.LD_LIBRARY_PATH
os.environ['LD_RUN_PATH'] = settings.LD_RUN_PATH

API_server = "carthage_thd_prod_support"
key = settings.ADIRONDACK_API_SECRET
dateparam = datetime.today() + timedelta(days=180)

# from __future__ import unicode_literals




# Create your tests here.
y = 'test'
# x = fn_lookuplist(y)
# print(x)

x = fn_lookup_accounts(API_server, key)
print(x)

# termlist = fn_lookup_terms(API_server, key)
# print(termlist)

