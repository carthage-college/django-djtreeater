django-djtreeater
=================

shell scripts for managing the data flow between the college and adirondack

##Quick Start

1. etc

##Dependent libraries

1. django-djimix
2. django-djauth
3. django-djtools
4. django==2.2.8
5. mysqlclient==1.4.5
6. pyodbc==4.0.27
7. pycryptodome==3.9.0
8. pysftp-0.2.9
9. requests==2.22.0

##Cron Tab

```
40 00 * * * (cd /data2/python_venv/3.6/djtreeater/ && . bin/activate && bin/python djtreeater/bin/misc_fees.py --database=cars 2>&1 | mail -s "[DJ Treeater] Adirondack miscellaneous fees" larry@carthage.edu) >> /dev/null 2>&1
45 00 * * * (cd /data2/python_venv/3.6/djtreeater/ && . bin/activate && bin/python djtreeater/bin/student_bio.py --database=cars 2>&1 | mail -s "[DJ Treeater] Adirondack student data" larry@carthage.edu) >> /dev/null 2>&1
50 00 * * * (cd /data2/python_venv/3.6/djtreeater/ && . bin/activate && bin/python djtreeater/bin/room_assignments.py --database=cars -run_mode auto 2>&1 | mail -s "[DJ Treeater] Adirondack room assignments" larry@carthage.edu) >> /dev/null 2>&1
```
