#! /usr/bin/env python

from distutils.core import setup

setup(name='batchacct-pub',
      description='Batch Accounting - Publishing',
      version='1.1',
      py_modules=['batchacct.join'],
      data_files=[('/etc/batchacct', ['pub', 'vos']),
                  ('/etc/cron.d', ['batchacct-pub.cron'])],
     )
