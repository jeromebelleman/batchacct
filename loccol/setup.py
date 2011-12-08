#! /usr/bin/env python

from distutils.core import setup

'''
Doesn't lay any egg and probably shouldn't be used to make
python-setup.py-installable packages as no post-install scripts have been
set up for that since no pre-uninstall script *can* be set up for that either.

In short: suitable only for python setup.py bdist_rpm
'''

setup(name='batchacct-loccol',
      description='Batch Accounting - Local Collection',
      version='1.1',
      py_modules=['batchacct.acct', 'batchacct.create'],
      data_files=[('/etc/init.d', ['batchacctd'])],
      options={'bdist_rpm': {'post_install':  'post_install',
                             'pre_uninstall': 'pre_uninstall'}}
     )
