#! /usr/bin/env python

'''Joins job information from local batch and CE table and prints APEL
message to stdout'''

# FIXME When there's only one message to publish, it's missed out (which
# isn't this bad because as soon as a second will come in they will both
# be consumed).

import sys
import common
import re
import optparse
import logging
import time, datetime
import stomp
import cx_Oracle

RESITE = re.compile('DC_(?P<site>[^_]+)')
REGUSER = re.compile('CN_(?P<guser>[^_]+)')
PORT = 61613
QUEUE = '/queue/apel'
#HEADER = 'APEL-individual-job-message: v0.2\n'
HEADER = 'APEL-individual-job-message: v1.1\n'
BUNCH = 1000 # SQL can't take more than that
NONLCG = '/local-nonlcg'
EPOCH = datetime.datetime(1970, 1, 1, 1, 0)
LOGFILE = '/var/log/batchacct/batchacct-pub.log'
GRIDCECOND = "(lrmsId IS NOT NULL OR queue NOT LIKE 'grid_%')"

class APELFieldError(Exception):
    def __init__(self, field):
        self.field = field

    def __str__(self):
        return "No value for %s" % self.field

class MsgLstnr:
    def on_error(self, headers, message):
        print 'received an error %s' % message

    def on_message(self, headers, message):
        print 'received a message %s' % message

class APELField:
    '''Maps APEL field to accounting DB column, providing processing functions
    if needs be'''
    i = 0

    def __init__(self, apelfield, col=None, val=None, fn=None, mty=None):
        self.apelfield = apelfield
        self.col = col
        self.val = val
        self.fn = fn
        self.mty = mty

        self.colidxs = []
        if self.col:
            for _ in col:
                self.colidxs.append(APELField.i)
                APELField.i += 1

    def __str__(self):
        return self.apelfield

def idx(dbcols, column):
    '''
    Return column position
    '''

    for f in dbcols:
        for i, c in zip(f.colidxs, f.col):
            if c == column:
                return i

### Callbacks ##################################################################

def factor(f):
    try:
        return int(f * factor.factorConstant)
    except TypeError:
        raise APELFieldError("hostFactor")

def wall(eventTime, startTime):
    try:
        delta = (eventTime - startTime)
        return delta.seconds
    except TypeError:
        raise APELFieldError("eventTime or startTime")

def cpu(utime, stime):
    try:
        return int(utime + stime)
    except TypeError:
        raise APELFieldError("ru_utime or ru_stime")

def ts(t):
    return int(time.mktime(t.timetuple()))

def fqan(chargedSAAP, userfqan):
    if userfqan is None: # ... 'cause the outer join didn't yield anything there
        # We're dealing with a local job
        if chargedSAAP is None:
            return NONLCG
        else:
            group = chargedSAAP.split('/')[1]

        try:
            vo = fqan.vogroups[group]
            return '/local-' + fqan.vogroups[group]
        except KeyError:
            if group not in fqan.unknowns:
                fqan.logger.warning("Don't know group %s -- Should I?" % group)
                fqan.unknowns.add(group)
            return NONLCG
    else:
        # We're dealing with a grid job
        # FQANs are expected semicolon-separated, not space-separated on
        # the APEL side.
        return userfqan.replace(' ', ';')

def jobid(jobid, idx):
    return str(jobid) + '-' + str(idx)

def inf(userfqan):
    if userfqan is None: # ... 'cause the outer join didn't yield anything there
        # We're dealing with a local job
        return 'local'
    else:
        # We're dealing with a grid job
        return 'grid'

### End of Callbacks ###########################################################

def send(logger, msg, j, mq=None, ssm=None):
    '''
    Send message to broker

    Not necessary when using the APEL SSM. 
    '''

    log = "Sending APEL message for %d events so far" % (j + 1)
    logger.info(log)
    if ssm != None:
        try:
            f = open(ssm + '/' + str(time.time()), 'a')
            f.write(msg)
            f.close()
        except IOError, e:
            logger.error(e)
            raise
    elif mq != None:
        mq.send(msg, destination=QUEUE)

def mark(updatecursor, pubs, t):
    '''
    Flag a job accounting record as published with its publication date
    '''

    update = "UPDATE %s SET %s.published = :t" % \
        (common.LOCALTAB, common.LOCALTAB)
    where = "WHERE %s.jobId IN (%s)" % \
        (common.LOCALTAB, ', '.join(str(p) for p in pubs))
    stmt = '%s %s' % (update, where)
    updatecursor.execute(stmt, [t])
    del pubs[:]

def main():
    # Read arguments
    p = optparse.OptionParser()
    help = "user/passwd@dsn-formatted accounting DB connection file path"
    p.add_option("-a", "--acctdbfile", help=help)
    help = "number of events per APEL message (default is %d)" % BUNCH
    p.add_option("-b", "--bunch", default=BUNCH, type='int', help=help)
    p.add_option("-c", "--conf", help="configuration file")
    help="VO file (containing VO-group mappings)"
    p.add_option("-v", "--vofile", help=help)
    p.add_option("-s", "--ssm", help="SSM home directory")
    help = "log file absolute path (defaults to %s)" % LOGFILE
    p.add_option("-l", "--logfile", help=help, default=LOGFILE)
    p.add_option("-m", "--msgbroker", help="message broker host")
    options, args = p.parse_args()

    # Set up logging
    #logging.basicConfig(level=logging.INFO, format=fmt)
    #h = logging.StreamHandler()
    h = logging.FileHandler(options.logfile)
    fmt = '%(asctime)s %(levelname)s %(message)s'
    h.setFormatter(logging.Formatter(fmt, common.LOGDATEFMT))
    logger = logging.getLogger(common.LOGGER)
    logger.addHandler(h)
    logger.setLevel(logging.INFO)

    if options.acctdbfile is None or options.vofile is None:
        p.print_help()
        return 1

    try:
        # Set configuration
        conf = {}
        if options.conf:
            f = open(options.conf, 'r')
            for l in f:
                key, val = l.split(None, 1)
                # Try to force it an integer
                try:
                    conf[key] = int(val[:-1]) # Remove trailing newline
                except ValueError:
                    conf[key] = val[:-1] # Remove trailing newline
            f.close()

        # Load VO-group mapping from file
        f = open(options.vofile, 'r')
        vogroups = {}
        for l in f:
            key, val = l.split()
            vogroups[key] = val
        f.close()
    except IOError, e:
        logger.error(e)
        return 1

    # Perform join, publish message, etc.
    connection = common.connect(logger, options.acctdbfile)
    cursor = connection.cursor()

    ce = common.CETAB
    local = common.LOCALTAB

    # FIXME Hmm...
    factor.factorConstant = conf['factorConstant']
    fqan.vogroups = vogroups
    fqan.logger = logger
    fqan.unknowns = set()

    fields = [
        APELField('Site', val=conf['site']),
        APELField('SubmitHost', ['%s.ceId' % ce], mty=conf['cluster']),
        # Was LocalJobID:
        APELField('LocalJobId', ['%s.jobId' % local, '%s.idx' % local],
                  fn=jobid),
        # Was LocalUserID: don't want to disclose this after all
        #APELField('LocalUserId', ['%s.userName' % local]),
        #APELField('GlobalUserName', ['%s.holderSubject' % ce]),
        #APELField('UserFQAN',
        #          ['%s.chargedSAAP' % local, '%s.attribute' % ce], fn=fqan),
        # Was UserFQAN:
        APELField('FQAN',
                  ['%s.chargedSAAP' % local, '%s.userFQAN' % ce], fn=fqan),
        APELField('WallDuration',
                  ['%s.eventTime' % local, '%s.startTime' % local], fn=wall),
        APELField('CpuDuration',
                  ['%s.ru_utime' % local, '%s.ru_stime' % local], fn=cpu),
        APELField('Processors', ['%s.numProcessors' % local]),
        APELField('NodeCount', ['%s.numExHosts' % local]),
        APELField('StartTime', ['%s.startTime' % local], fn=ts),
        APELField('EndTime', ['%s.eventTime' % local], fn=ts),
        APELField('MemoryReal', ['%s.maxRMem' % local]),
        APELField('MemoryVirtual', ['%s.maxRSwap' % local]),
        # Was ScalingFactorUnit:
        APELField('ServiceLevelType', val=conf['unit']),
        # Was ScalingFactor:
        APELField('ServiceLevel', ['%s.hostFactor' % local], fn=factor),
        APELField('Infrastructure', ['%s.userFQAN' % ce], fn=inf),
             ]

    conf['fields'] = conf['fields'].split()
    fields = [f for f in fields if f.apelfield in conf['fields']]

    # But why not use DBCol there too? Because it's not about creating
    # a table, because we don't care about types but we are, however,
    # constantly dealing with fields and columns with different names:
    # what we need here is a mapping, not a template describing a table
    # and its data.

############# SELECT ... FOR UPDATE -- Very Slow ###############################
#    cols = filter(lambda c: c.col != None, fields)
#
#    select = "SELECT %s" % ', '.join("%s.%s" % (c.tab, c.col) for c in cols)
#    tables = "FROM %s JOIN %s ON %s.jobId = %s.lrmsJobId" % \
#        (common.LOCALTAB, common.CETAB, common.LOCALTAB, common.CETAB)
#    where = "WHERE %s.published = 0" % \
#        (common.LOCALTAB)
#    # A cursor is deadly in terms of performance
#    #curs = 'CURSOR pub IS %s %s %s FOR UPDATE OF %s.published;' % \
#    #    (select, tables, where, common.LOCALTAB)
#    curs = 'CURSOR pub IS %s %s %s;' % (select, tables, where)
#
#    output = "DBMS_OUTPUT.PUT_LINE(%s);" % \
#        ' || '.join("c.%s" % (c.col) for c in cols)
#    update = "UPDATE %s SET %s.published = 1 WHERE CURRENT OF pub;" % \
#        (common.CETAB, common.LOCALTAB)
#    update = ''
#   
#    stmt = '''\
#DECLARE
#%s
#BEGIN
#FOR c IN pub LOOP
#  %s
#  %s
#END LOOP;
#END;
#''' % (curs, output, update)
#
#    try:
#        cursor.callproc("dbms_output.enable", [None])
#        logger.info("Executing cursor statement")
#        cursor.execute(stmt)
#        
#        line = cursor.var(cx_Oracle.STRING)
#        status = cursor.var(cx_Oracle.NUMBER)
#        while True:
#            cursor.callproc("dbms_output.get_line", (line, status))
#            if status.getvalue() != 0:
#                break
#            else:
#                print line.getvalue()
#
#        logger.info("Looping through cursor")
#        for i, row in enumerate(cursor):
#            if i % 10:
#                print i
#    except cx_Oracle.DatabaseError, e:
#        logger.error(e)
############# End of SELECT ... FOR UPDATE -- Very Slow ########################

    logger.info("Joining local and CE job event records")

    # Only DB columns
    dbcols = [f for f in fields if f.col != None]

    # Useful row indexes for later on
    jobIdIdx = idx(dbcols, '%s.jobId' % common.LOCALTAB)
    eventTimeIdx = idx(dbcols, '%s.eventTime' % common.LOCALTAB)
    lrmsJobIdIdx = idx(dbcols, '%s.lrmsId' % common.CETAB)

    # Write SELECT statement
    try:
        select =  "SELECT %s" % ', '.join(', '.join(c.col) for c in dbcols)
        tables = "FROM %s LEFT JOIN %s" % (common.LOCALTAB, common.CETAB)
        on = "ON %s.jobId = %s.lrmsId" % (common.LOCALTAB, common.CETAB)
        where = "WHERE published = :e AND %s AND %s AND %s" % \
            (GRIDCECOND, common.STTCOND, common.CPUCOND)
        stmt = '%s %s %s %s' % (select, tables, on, where)
        cursor.execute(stmt, [EPOCH, EPOCH])
    except cx_Oracle.DatabaseError, e:
        logger.error("Couldn't join local and CE job records: %s" % e)
        return 1

    # Connect to message broker
    if options.ssm == None:
        logger.info("Connecting to message broker on %s" % options.msgbroker)
        try:
            mq = stomp.Connection(host_and_ports=[(options.msgbroker, PORT)])
            mq.set_listener('', MsgLstnr())
            mq.start()
            mq.connect()
        except stomp.exception.ReconnectFailedException, e:
            logger.error("Couldn't connect to message broker")
            return 1
    else:
        mq = None

    # Retrieve rows and send messages to broker as you go
    logger.info("Performing join between %s and %s",
                common.LOCALTAB, common.CETAB)
    msg = HEADER
    pubs = []
    updatecursor = connection.cursor()
    start, end = 0, 0
    t = datetime.datetime.now()
    try:
        j = 0 # In case the cursor is empty
        for j, row in enumerate(cursor):
            for c in fields:
                # Prefer DB value than constant because we absolutely need to
                # increment i if we're dealing with a DB column for later on
                if c.col != None:
                    if c.fn is None:
                        # If there's no function assigned, we can't be dealing 
                        # with more than one DB column and we can therefore 
                        # take just the one from the first (and assumingly 
                        # only) index.
                        val = row[c.colidxs[0]]
                    else:
                        # If there's an assigned function, use it and pass it
                        # all the indexed columns as arguments.

                        # Assigned functions should typically raise an
                        # APELFieldError if any of the parameter is None.
                        # It shouldn't happen if the DB does its job, hence
                        # the exception.
                        val = c.fn(*(row[i] for i in c.colidxs))

                    # At this stage, val could be None in the c.fn is None
                    # case above. That's why we need to check next.

                    if val != None:
                        msg += '%s: %s\n' % (c, val)
                    elif val is None and c.mty != None:
                        # If val is None and it's a compulsory APEL field,
                        # give it the default value planned for compulsory
                        # fields for which we have no data.
                        msg += '%s: %s\n' % (c, c.mty)
                    else:
                        # Value may be None, so we shave it off the message
                        # entirely.
                        pass

                    # This bit of code didn't really bother whether or not
                    # the left join yielded None values on the right hand,
                    # since all we really care about at the end of the day is
                    # not have None values (or missing values, in fact, since
                    # I shave None values off) where you don't want them.

                elif c.val != None:
                    # Constant, default value if we don't mean to look at the 
                    # DB for this field.
                    msg += '%s: %s\n' % (c, c.val)
                else:
                    # We're not looking at the DB and we didn't plan any 
                    # constant, default value: we have a problem.
                    raise APELFieldError(c)

                # Record first and last eventTime
                end = row[eventTimeIdx]
                if start == 0:
                    start = end

            # Add jobId to temporary array for later flagging
            pubs.append(row[jobIdIdx])
            if (j + 1) % options.bunch == 0:
                msg += '%%\n'
                send(logger, msg, j, mq=mq, ssm=options.ssm)
                msg = HEADER

                # Mark this bunch as published
                mark(updatecursor, pubs, t)
            else:
                msg += '%%\n'

        # Send last bit if any
        if msg != HEADER:
            send(logger, msg, j, mq=mq, ssm=options.ssm)
            mark(updatecursor, pubs, t)
    except cx_Oracle.DatabaseError, e:
        logger.error("Couldn't mark some records as published: %s" % e)
        return 1
    except stomp.exception.NotConnectedException:
        logger.error("Lost connection to message broker")
        return 1
    except APELFieldError, e:
        logger.error(e)
        return 1

    if end == 0:
        logger.info("Didn't send any APEL message")
    else:
        log = "Sent APEL messages for %d events between %s and %s to %s"
        logger.info(log % (j + 1, start, end, QUEUE))

    # Disconnect from message broker
    if mq != None:
        mq.disconnect()

    # Commit
    try:
        connection.commit()
    except cx_Oracle.DatabaseError, e:
        logger.error("Couldn't commit: %s" % e)
        return 1

    logger.info("Done")

if __name__ == '__main__':
    sys.exit(main())
