#! /usr/bin/env python

'''
Notice LSF accounting event records as they show up, parse them
and send them to the accounting database. It's probably worth
reading from https://github.com/seb-m/pyinotify/wiki/Tutorial and
pylsf/examples/lsb_geteventrec.py to understand how all this works.
'''

import os
import sys
import time
import optparse
import logging
import datetime
import pyinotify
from pyinotify import IN_CREATE, IN_MODIFY, IN_MOVED_FROM, IN_MOVED_TO
import cx_Oracle
import pylsf
import common

PIDFILE = '/var/run/batchacctd.pid'
LOGFILE = '/tmp/batchacct.log'

# FIXME: add catch-up from old logfiles once we know the actual format

class EventHandler(pyinotify.ProcessEvent):
    '''
    Handle inotify events, when accounting event records are appended to the
    accounting file and when the accounting file gets logrotated (i.e. when
    it is renamed and a new file with the original name is created).
    '''

    def __init__(self, logger, acctfile, recs,
                 connection, heartbeatdelta=common.HBDELTA, dryrun=False):
        '''
        Instantiation method.
        
        Expects an iterable lsb_geteventrec instance
        (well, lsb_geteventrec instances are iterable by design anyway)
        and a database cursor.
        '''

        self.acctfile = acctfile
        self.recs = recs
        self.connection = connection
        self.logger = logger
        self.insertc = 0
        self.errorc = 0
        self.heartbeatdelta = heartbeatdelta
        self.heartbeat = datetime.datetime.today()
        self.dryrun = dryrun

    def process_IN_MODIFY(self, event):
        '''
        Handles a file modification event, typically when one or more event
        records have been appended to the accounting file. The new records
        are sent to the accounting database here.
        '''

        if event.name == os.path.basename(self.acctfile):
            if self.dryrun:
                self.logger.info("Would normally send records")
            else:
                insertc, errorc, heartbeat = common.insert(self.logger,
                                                           common.LOCALTAB,
                                                           self.recs,
                                                           self.connection,
                                                           self.insertc,
                                                           self.errorc,
                                                           self.heartbeat,
                                                           self.heartbeatdelta)
                self.insertc = insertc
                self.errorc = errorc
                self.heartbeat = heartbeat

    def process_IN_MOVED_FROM(self, event):
        '''
        Handles a file renaming event, which happens on the first stage of a
        logrotation. The event is merely notified here and no further action
        is taken.
        '''
        if event.name == os.path.basename(self.acctfile):
            self.logger.info('Logrotated %s' % (event.name))

    def process_IN_CREATE(self, event):
        '''
        Handles a file creation event, which happens on the second and last
        stage of a logrotation. A new lsb_geteventrec instance is created
        (i.e. the new accounting file with the original accounting name is
        opened again).
        '''

        # Don't wm.close() here, as it would close all watches. Don't
        # wm.del_watch() either because I don't think you'd be able to add
        # the watch again without restarting the notifier loop (and I don't
        # know if it's possible to restart the notifier loop either). And
        # don't wm.rm_watch() because it makes pyinotify complain. In fact,
        # leave the watches alone and just make sure you don't have any watch
        # looking at files directly, as opposed to looking at directories.

        if event.name == os.path.basename(self.acctfile):
            # Shouldn't be necessary but read any possibly unhandled event
            # records from the old file, in case new entries have been
            # appended to it, it's been renamed and the new file has been
            # created atomically.
            # It's possible to do it this way because seems to be targeting
            # inodes, not file names.
            if self.dryrun:
                self.logger.info("Would normally send records")
            else:
                insertc, errorc, heartbeat = common.insert(self.logger,
                                                           common.LOCALTAB,
                                                           self.recs,
                                                           self.connection,
                                                           self.insertc,
                                                           self.errorc,
                                                           self.heartbeat,
                                                           self.heartbeatdelta)
                self.insertc = insertc
                self.errorc = errorc
                self.heartbeat = heartbeat

            self.logger.info('Created %s' % event.name)
            if os.path.isfile(self.acctfile):
                # Previous file implicitly closed here when the lsb_geteventrec
                # is deallocated
                self.recs = pylsf.lsb_geteventrec(self.acctfile)
            else:
                # Report to log
                strerr = "No such file or directory"
                self.logger.error("Couldn't open acct file: %s" % strerr)

def main():
    '''
    Set up pylsf to read event records and set up pyinotify to notice them
    as they come. Nothing expected, nothing returned.
    '''

    # Read arguments
    p = optparse.OptionParser()
    help = "user/passwd@dsn-formatted database connection file absolute path"
    p.add_option("-c", "--connfile", help=help)
    help = "LSF accounting log file absolute path"
    p.add_option("-a", "--acctfile", help=help)
    help = "PID file absolute path (defaults to %s)" % PIDFILE
    p.add_option("-p", "--pidfile", help=help, default=PIDFILE)
    help = "log file absolute path (defaults to %s)" % LOGFILE
    p.add_option("-l", "--logfile", help=help, default=LOGFILE)
    help = "how many minutes between log heart beats (defaults to %d)" % \
           common.HBDELTA
    p.add_option("-b", "--heartbeatdelta", type='int',
                 help=help, default=common.HBDELTA)
    help = "Don't touch the DB"
    p.add_option("-d", "--dryrun", action='store_true', help=help)
    options, args = p.parse_args()

    # Set up logging
    #fmt = '%(asctime)s %(levelname)s %(message)s'
    #logging.basicConfig(level=logging.INFO, format=fmt,
    #                    filename='/tmp/acct.log')

    #h = logging.handlers.SysLogHandler(address='/dev/log')
    h = logging.FileHandler(options.logfile)
    #fmt = "%(name)s: %(levelname)s %(message)s"
    fmt = "%(asctime)s %(name)s: %(levelname)s %(message)s"
    h.setFormatter(logging.Formatter(fmt, common.LOGDATEFMT))
    logger = logging.getLogger(common.LOGGER)
    logger.addHandler(h)
    logger.setLevel(logging.INFO)

    # Daemonise
    try:
        if common.daemonise(logger, options.pidfile) > 0:
            return 0
    except common.DaemonError:
        return 1

    try:
        # Set up accounting DB connection
        if options.dryrun:
            logger.info("Would normally connect to DB")
            connection = None
        else:
            connection = common.connect(logger, options.connfile)

        # Set up LSF
        acctfile, recs = common.accounting(logger, options.acctfile)
    except common.AcctDBError, e:
        logger.error(e)
        return 1

    # Set up pyinotify
    logger.info("Pyinotify will be watching %s" % acctfile)
    wm = pyinotify.WatchManager()
    handler = EventHandler(logger, acctfile, recs, connection,
                           options.heartbeatdelta, options.dryrun)
    notifier = pyinotify.Notifier(wm, handler)

    # IN_MOVE_SELF isn't much use to me here, it seems: when watching a file,
    # it croaks an error upon events and when watching a directory it doesn't
    # notice any change. Also note that IN_MOVED_FROM only seems to work
    # on directories.

    # If you directly watch a file for modification (as opposed to a directory)
    # you'll have trouble turning your stare after a logrotation. It's much
    # easier to watch the directory as it's done here:
    try:
        wm.add_watch(os.path.dirname(acctfile),
                     IN_MODIFY | IN_MOVED_FROM | IN_CREATE)
    except pyinotify.WatchManagerError, e:
        logger.error(e)
        return 1

    # Loop and dispatch events forever
    try:
        notifier.loop()
    except pyinotify.NotifierError, e:
        logger.error(e)
        return 1

if __name__ == '__main__':
    sys.exit(main())
