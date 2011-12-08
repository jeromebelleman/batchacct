#! /usr/bin/env python

from distutils.core import setup

setup(name='batchacct-cecol',
      description='Batch Accounting - CE Collection',
      version='1.1',
      py_modules=['batchacct.whisk'],
      data_files=[
                  ('/etc/init.d', ['batchacct-cecold']),
                 ]
     )
