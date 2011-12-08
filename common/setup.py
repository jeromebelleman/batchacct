#! /usr/bin/env python

from distutils.core import setup

setup(name='batchacct-common',
      description='Batch Accounting - Common Files',
      version='1.1',
      py_modules=['batchacct.common'],
      data_files=[
                    ('/etc/batchacct', ['connection']),
                    ('/var/log/batchacct', []),
                    ('/var/run/batchacct', []),
                 ]
     )
