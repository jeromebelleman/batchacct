#! /usr/bin/env python

'''
Notice BLAH accounting event records as they show up, parse them and send
them to the DB.
'''

import sys
import os
import logging
import re
import optparse
import pyinotify
import datetime
import time
from pyinotify import IN_CREATE, IN_MODIFY
import common

PIDFILE = '/var/run/batchacct/batchacct-cecold.pid'
LOGFILE = '/var/log/batchacct/batchacct-cecold.log'
REFILENAME = re.compile('^blahp.log-\d{8}$')

def latest(acctdir):
    '''
    Expect a directory path to look for BLAH files into and return the latest
    BLAH file name.
    '''
    # List BLAH files
    entries = [(e, os.stat('%s/%s' % (acctdir, e)))
               for e in os.listdir(acctdir)
               if REFILENAME.match(e)]

    if len(entries) == 0:
        raise common.AcctError("Couldn't list BLAH accounting files")

    # Sort them by modification time
    entries.sort(key=lambda s: s[1].st_mtime)

    # Return latest BLAH file's name
    return entries[-1][0]

class EventHandler(pyinotify.ProcessEvent):
    # Not quite the same as the local collector one (e.g. I can't just sit 
    # there looking at the same file all the time because there's no log 
    # rotation). I can't really just refactor things around.

    def __init__(self, logger, acctdir, acctfile,
                 connection, heartbeatdelta=common.HBDELTA):
        self.logger = logger
        self.acctdir = acctdir
        self.acctfile = acctfile
        self.connection = connection
        self.heartbeatdelta = heartbeatdelta

        self.heartbeat = datetime.datetime.today()
        self.insertc = 0
        self.errorc = 0
        self.offset = 0

    def process_IN_MODIFY(self, event):
        '''
        Handles a file modification event, typically when one or more event
        records have been appended to the accounting file. The new records
        are sent to the database here.
        '''

        f = open(self.acctdir + '/' + self.acctfile)
        recs = common.parse(f, self)

        insertc, errorc, heartbeat = common.insert(self.logger, common.CETAB,
                                                   recs, self.connection,
                                                   self.insertc, self.errorc,
                                                   self.heartbeat,
                                                   self.heartbeatdelta)

        f.close()

        self.insertc = insertc
        self.errorc = errorc
        self.heartbeat = heartbeat

    def process_IN_CREATE(self, event):
        '''
        Handles a file creation event, which happens pretty much every day. 
        The previously read BLAH file is closed and the new one reopened.
        '''

        try:
            l = latest(self.acctdir)
            if l == self.acctfile:
                return
        except common.AcctError, e:
            logger.error(e)
            raise

        # Flush any missed out records into the DB
        f = open(self.acctdir + '/' + self.acctfile)
        recs = common.parse(f, self)

        insertc, errorc, heartbeat = common.insert(self.logger, common.CETAB,
                                                   recs, self.connection,
                                                   self.insertc, self.errorc,
                                                   self.heartbeat,
                                                   self.heartbeatdelta)

        f.close()

        self.insertc = insertc
        self.errorc = errorc
        self.heartbeat = heartbeat

        # Set last modified file
        self.acctfile = latest(self.acctdir)
        self.offset = 0

        # Report
        self.logger.info("Pyinotify will now be watching %s" % self.acctfile)

def getjobs(ora, my, cetable):
    '''
    Get (all) jobs from CE

    Expects:
    - a cx_Oracle.Connection instance to the accounting DB
    - a MySQLdb.connections.Connection instance to the CE DB
    - the job table name on the CE DB

    Returns a MySQLdb.result to be repeatedly read with fetch_row()

    This function would normally no longer be used since one is supposed to
    directly read from the BLAH file, not from the CE DB.
    '''

    # What's this CE's CREAM URL?
    my.query('SELECT creamURL FROM %s GROUP BY creamURL' % cetable)

    # We're assuming the CREAM URL is CE-unique
    r = my.use_result()
    creamurl = None
    while True:
        row = r.fetch_row() 
        if len(row) > 0:
            if creamurl is None:
                ((creamurl,),) = row
            else:
                # Our assumption is wrong. Need to rethink the algorithm.
                msg = 'More than one distinct CREAM URL in this CE'
                raise common.AcctDBError(msg)
        else:
            # This row is an empty tuple -- We're done.
            break

    # What's the last job, i.e. the job with the maximum id for this ceId?
    stmt = 'SELECT MAX(id) FROM %s WHERE creamURL = :creamurl'
    oracursor = ora.cursor()
    oracursor.execute(stmt % common.CETAB, [creamurl])
    maxid, = oracursor.fetchone()

    # Get resulting (row by row) jobs
    cols = filter(lambda c: c.dftval is None, common.CETAB)
    select = 'SELECT %s' % ', '.join([c.col for c in cols])
    where = "WHERE lrmsAbsLayerJobId != 'N/A'"
    idcond = ''
    #if maxid is not None:
    #    idcond = "AND id > '%s'" % maxid
    my.query("%s FROM %s %s %s" % (select, cetable, where, idcond))
    r = my.use_result()

    return r

def putjobs(ora, jobs):
    '''
    Add jobs to accounting DB

    Expects:
    - a cx_Oracle.Connection instance to the accounting DB
    - a MySQLdb.result as returned by getjobs()

    Doesn't return anything
    '''
    import cx_Oracle

    # Get cursor
    cursor = ora.cursor()

    # Evaluate record for each field
    while True:
        r = jobs.fetch_row(how=1)
        if len(r) > 0:
            r, = r
            # Evaluate against actual value to see what we're up against
            for c in common.CETAB:
                c.eval(r)

            # Lay out statement
            fmt = 'INSERT INTO %s VALUES (%s)'
            tab = common.CETAB
            stmt = fmt % (tab, ', '.join([c.param for c in common.CETAB]))

            # Execute statement
            l = filter(lambda v: v is not None, [c.val for c in common.CETAB])
            try:
                cursor.execute(stmt, l)
                # FIXME: distinguish interesting errors from uniqueness error
            except cx_Oracle.DatabaseError, e:
                pass
                #logging.error(str(e)[:-1])
                #for c in common.CETAB:
                #    print "%s %s '%s'" % (c.col, c.type, str(c.val)[:60])
                #print
            else:
                pass
                #i += 1
        else:
            # This row is an empty tuple -- We're done.
            break
    ora.commit()

def main():
    # Read arguments
    p = optparse.OptionParser()
    help = """BLAH accounting directory absolute path -- if set, it's \
supposed to run on each CE (as opposed to centrally by connecting to \
each CE's DB)"""
    p.add_option("-a", "--acctdir", help=help)
    help = "user/passwd@dsn-formatted accounting connection file path"
    p.add_option("-c", "--connfile", help=help)
    help = "user/passwd-formatted configuration manager DB connection file path"
    p.add_option("-d", "--mgrfile", help=help)
    help = "configuration manager string: dbtable,cluster,subcluster"
    p.add_option("-m", "--manager", help=help)
    help = "user/passwd-formatted CE DB connection file path"
    p.add_option("-e", "--cefile", help=help)
    p.add_option("-t", "--cetable", help="CE DB table")
    help = "override accounting table name"
    p.add_option("-n", "--name", help=help)
    help = "log file absolute path (defaults to %s)" % LOGFILE
    p.add_option("-l", "--logfile", help=help, default=LOGFILE)
    help = "PID file absolute path (defaults to %s)" % PIDFILE
    p.add_option("-p", "--pidfile", help=help, default=PIDFILE)
    help = "how many minutes between log heart beats (defaults to %d)" % \
           common.HBDELTA
    p.add_option("-b", "--heartbeatdelta", type='int',
                 help=help, default=common.HBDELTA)
    options, args = p.parse_args()

    # Set up logging
    h = logging.FileHandler(options.logfile)
    fmt = '%(asctime)s %(levelname)s %(message)s'
    h.setFormatter(logging.Formatter(fmt, common.LOGDATEFMT))
    logger = logging.getLogger(common.LOGGER)
    logger.addHandler(h)
    logger.setLevel(logging.INFO)

    # Get new jobs
    logger.info("Connecting to accounting DB...")
    acct = common.connect(logger, options.connfile)

    if options.acctdir is None:
        logger.info("Connecting to configuration manager DB...")
        mgr = common.connect(logger, options.mgrfile)
        logger.info("Retrieving hosts from configuration manager DB...")
        table, cluster, subcluster = \
            (s.strip() for s in options.manager.split(','))
        hosts = common.gethosts(mgr, table, cluster, subcluster)

        if options.name:
            common.CETAB.name = options.name

        for h in hosts:
            try:
                logger.info("Connecting to %s..." % h)
                ce = common.myconnect(logger, options.cefile, host=h)
                logger.info("Getting jobs from %s..." % h)
                jobs = getjobs(acct, ce, options.cetable)
                logger.info("Sending new jobs from %s to accounting DB..." % h)
                putjobs(acct, jobs)
                logger.info("Done.")
            except common.CEDBError, e:
                msg = "%s: %s, but let's forget about it for now"
                logger.warning(msg % (h, e))
    else:
        # Daemonise
        try:
            if common.daemonise(logger, options.pidfile) > 0:
                return 0
        except common.DaemonError:
            return 1

        try:
            # Set up accounting DB connection
            connection = common.connect(logger, options.connfile)

            # Open accounting file
            try:
                l = latest(options.acctdir)
            except OSError, e:
                logger.error(e)
                raise

            # Set up pyinotify
            logger.info("Pyinotify will be watching %s" % l)
            wm = pyinotify.WatchManager()
            handler = EventHandler(logger, options.acctdir, l, connection,
                                   options.heartbeatdelta)
            notifier = pyinotify.Notifier(wm, handler)

            # BLAH accounting files don't seem to be subject to logrotation:
            # new files with a new name are simply created. Therefore, we're
            # only interested in file changes (i.e. files being appended
            # new records) and file creation -- not file moves.
            wm.add_watch(options.acctdir, IN_MODIFY | IN_CREATE)

            # Loop and dispatch events forever
            notifier.loop()
        except common.AcctError, e:
            logger.error(e)
            return 1
        except pyinotify.WatchManagerError, e:
            logger.error(e)
            return 1
        except pyinotify.NotifierError, e:
            logger.error(e)
            return 1

    return 0

if __name__ == '__main__':
    sys.exit(main())
