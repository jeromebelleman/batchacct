#! /usr/bin/env python

import os
import sys
import re
import time
import logging
from datetime import datetime, timedelta, date
import cx_Oracle

RE = "^(?P<username>[^/]+)/(?P<password>[^@]+)@(?P<dsn>.+)$"
DFTACCT = 'lsb.acct'

TYPERE = re.compile('(?P<type>\w+)(?:\((?P<len>\d+)\))?(?P<notnull> NOT NULL)?')
ATTRRE = re.compile('.*Attribute\s*:(?P<attr>[^\\n]*)\n.*', re.S)
HOLDERRE = re.compile('.*Holder Subject\s*:(?P<holder>[^\\n]*)\n.*', re.S)
LOGGER = 'batchacct'
HBDELTA = 180
LOGBUNCH = 10000
ORAIDLIM = 31 # Max characters for Oracle identifier
LOGDATEFMT = '%Y-%m-%d %H:%M:%S'

# Error formats
INSERTERR = "Couldn't insert record: %s"
COMMITERR = "Couldn't commit: %s"

# Whisk parser
RERECORD = re.compile('"(\w+=.*?)"')
TFMT = '%Y-%m-%d %H:%M:%S'
# Blah accounting file fields
TIMESTAMP = 'timestamp'
CEID = 'ceID'
LRMSID = 'lrmsID'
USERFQAN = 'userFQAN'

# Common conditions and SQL bits
CPUTIME = "(ru_utime + ru_stime) * hostFactor / numProcessors"
WALLTIME = "((eventTime - startTime) * 24 * 60 * 60 * hostFactor)"
EVTCOND = "eventTime > :t"
STTCOND = "startTime != :e"
TSHCOND = "%s / %s < :l" % (CPUTIME, WALLTIME)
CPUCOND = "ru_stime != -1 AND ru_utime != -1"
# Infinite efficiency doesn't exist in our world. (Does it?)
DIV0COND = "eventTime != startTime"
HOSTCOND = "hostFactor != 0" # Definitely wrong, but it happens

class DBTab:
    def __init__(self, name, cols, pk=[], idxs=[]):
        self.name = str(name)
        self.cols = cols
        self.pk = pk
        self.idxs = idxs
        DBCol.pos = 1

    #def __iter__(self):
    #    return iter(self.cols)

    # Useful for debugging with print
    def __repr__(self):
        return str([c.val for c in self.cols])

    def __str__(self):
        return self.name

    def __getitem__(self, key):
        if isinstance(key, int) or isinstance(key, slice):
            return self.cols[key]
        elif isinstance(key, str):
            for c in self.cols:
                if key == c.col:
                    return c
                    break
            else:
                raise KeyError()
        else:
            raise KeyError(type(key))

    def __add__(self, other):
        return self.cols + other.cols

    def __len__(self):
        return len(self.cols)

class DBCol:
    '''
    Defines a mapping between a PyLSF event record and a database column.

    Used in the context of centralising event record fields and database
    column management.
    '''

    pos = 1 # Initialised here and updated by DBTab

    def __init__(self, col, type, fn=None, dftval=None, src=None):
       '''
       Initialize a DBCol instance and increment query parameter position.

       Expects:
       - The column name string (e.g. event_type, version_number,
         jobid, etc.)
       - A fully qualified column type string (e.g. VARCHAR2(255)
         NOT NULL, NUMBER, DATE).
       - An optional function for event record values which need
         post-processing (e.g. function len to count the number of
         executing hosts if all we have is a sequence of executing
         hosts).
       - An optional value setting the DB column to a default value:
         this implicitly assumes that values for this column will not be fed
         from the accounting file but separately, and therefore that no such
         field should be expected from the accounting file. Typical example:
         the 'published' column.
       - Original name of the field in the accounting file.

       It's worth noting that this class tries to be clever with
       types, which is why column types should be specified in a fully
       qualified sort of way. For instance, if the type passed is
       'DATE', the returned value will be NULL if the date in the event
       record is missing; if the type passed is 'DATE NOT NULL', the
       returned value will be the Epoch so as to avoid upsetting the DB.
       Likewise if the type is 'VARCHAR2(255)', the event record value
       will be sliced down to 255 characters.
       '''
       self.pos = DBCol.pos
       DBCol.pos += 1 # Keep track of parameter positioning 
       self.col = col
       self.type = type
       self.fn = fn
       self.dftval = dftval
       self._src = src

    def src(self):
        '''
        Originating field name as seen in the accounting file
        '''
        if self._src is None:
            return self.col
        else:
            return self._src
    src = property(src)

    #@getter # If we ran Python 2.6
    def param(self):
        '''
        Parameter (e.g. :arg_42) or the string 'NULL' if the value is NULL.
        '''
        type, len, nullable = parsetype(self.type)
        if self.val is not None:
            return ":arg_%d" % self.pos
        else:
            return 'NULL'
    param = property(param)

    def eval(self, rec):
        '''
        Compute value associated with this DBCol from an event record and
        set operator (= or IS) approriately.

        Expects a whole event record list. (I.e. with all the values, even
        those having nothing to do with this DBCol. It's this function's
        job to sort it all out.)
        '''
        # Get type and attributes
        type, len, nullable = parsetype(self.type)

        # Decide what to do with the raw value
        if self.dftval is None: # Inline if-else doesn't work on Python 2.4
            self.val = rec[self.src]
        else:
            self.val = self.dftval

        self.op = '='
        if self.fn:
            self.val = self.fn(self.val)
        if type.upper() == 'VARCHAR2' and len is not None:
            self.val = self.val[:len]
        if self.val == '':
            # Empty string are regarded as NULL on Oracle. Misleading,
            # yes, but there you go.
            self.val = None 
            self.op = 'IS'

class DaemonError(Exception):
    pass

class AcctError(Exception):
    '''
    Top-level accounting exception
    '''
    pass

class CEDBError(AcctError):
    '''
    CE database-related exception
    '''
    pass

class MissingAcctError(AcctError):
    '''
    Basic exception raised when the accounting file is missing.
    '''

    def __init__(self, filename):
        self.filename = filename

    def __str__(self):
        return "Missing accounting file: %s" % self.filename

class AcctDBError(AcctError):
    '''
    Basic exception raised when there's a problem with the accounting DB.
    '''

    def __str__(self):
        return "Accounting database error: %s" % self.args

def ftab(lines, header):
    '''
    Format tables
    '''

    cols = [0 for _ in header]

    # Measure widths
    lines.insert(0, header)
    for l in lines:
        for i, c in enumerate(l):
            if len(str(c)) > cols[i]:
                cols[i] = len(str(c))
    lines.pop(0)

    # Header
    print ' '.join([str(c).ljust(w) for c, w in zip(header, cols)])
    for c in cols:
        print ''.join(['-' for _ in range(c)]),
    print

    # Lines
    for l in lines:
        print ' '.join([str(c).ljust(w) for c, w in zip(l, cols)])

def ots(unixts, nullable=False):
    '''
    Return a datetime.datetime instance given a parameter-passed integer
    UNIX timestamp.

    Whether the table value is nullable or not may be specified as second
    argument; in the latter case the epoch will be returned as was done in
    the former accounting table.
    '''
    # FIXME Epoch instead NULL, eh? Well, well, well, is that such a good
    # idea, I wonder...
    fmt = "%Y-%m-%d %H:%M:%S"
    if unixts <= 0 and nullable:
        return None
    elif unixts <= 0 and not nullable:
        return datetime.fromtimestamp(0)
    else:
        return datetime.fromtimestamp(unixts)

def offsetOts(unixts, nullable=False):
    '''
    Same as ots, but handles the old accounting DB where times are
    timezone-offset.
    '''
    t = ots(unixts, nullable)

    if t is None:
        return t
    elif unixts <= 0:
        # Oracle Epoch's seems to be at midnight, not 1 o'clock
        return t - timedelta(hours=1)
    else:
        return t - timedelta(hours=2)

def overflow(v):
    '''
    Expect a numeric value an return it the way Oracle seems to do it,
    overflows and all.
    '''
    if v > sys.maxint:
        return -sys.maxint - 1
    else:
        return v

def rchop(cmd):
    '''
    Returns up to the first 200 characters of the command string passed
    as argument.

    Useful for comparing accounting files with the old accounting DB where
    command lines were trimmed that short.
    '''
    return cmd[:200]

def lchop(cmd):
    '''
    Returns up to the last 200 characters of the command string passed
    as argument.

    Useful for comparing accounting files with the old accounting DB where
    command lines were trimmed that short.
    '''
    return cmd[-200:]

def catTrim(seq):
    '''
    Expect a string-representable sequence and just return the first item
    followed by an ellipsis, as done on the old accounting DB.
    '''
    if len(seq) > 1:
        return seq[0] + '...'
    elif len(seq) > 0:
        return seq[0]
    else:
        return ''

def cat(seq):
    '''
    Expect a string-representable sequence and return a single concatenated
    string of its items separated with spaces.
    '''
    return ' '.join(seq)

def attr(delegproxy):
    '''
    Extract 'Attribute' field from delegationProxyInfo column

    Expects full-text delegationProxyInfo string and returns the 'Attribute'
    field value.
    '''
    m = ATTRRE.match(delegproxy)
    if m is not None:
        return m.group('attr').strip()

def holder(delegproxy):
    '''
    Extract 'Holder Subject' field from delegationProxyInfo column

    Expects full-text delegationProxyInfo string and returns the 'Holder
    Subject' field value.
    '''
    m = HOLDERRE.match(delegproxy)
    if m is not None:
        return m.group('holder').strip()

LOCALTAB = DBTab('loc',
    (
     DBCol('eventType', 'VARCHAR2(255) NOT NULL'),
     DBCol('version', 'VARCHAR2(255) NOT NULL'),
     DBCol('eventTime', 'DATE NOT NULL', ots),
     DBCol('jobId', 'NUMBER(10) NOT NULL'),
     DBCol('userId', 'NUMBER(7) NOT NULL'),
     DBCol('userName', 'VARCHAR2(255) NOT NULL'),
     DBCol('options', 'NUMBER(10)'),
     DBCol('numProcessors', 'NUMBER'),
     DBCol('jStatus', 'NUMBER(5) NOT NULL'),
     DBCol('submitTime', 'DATE NOT NULL', ots),
     DBCol('beginTime', 'DATE', ots),
     DBCol('termTime', 'DATE', ots),
     DBCol('startTime', 'DATE NOT NULL', ots),
     DBCol('queue', 'VARCHAR2(255) NOT NULL'),
     DBCol('resReq', 'VARCHAR2(255)'),
     DBCol('fromHost', 'VARCHAR2(255)'),
     DBCol('numAskedHosts', 'NUMBER'),
     DBCol('askedHosts', 'VARCHAR2(255)', cat),
     DBCol('hostFactor', 'NUMBER(5,2) NOT NULL', lambda v: round(v, 2)),
     DBCol('numExHosts', 'NUMBER'),
     DBCol('execHosts', 'VARCHAR2(255)', cat),
     DBCol('jobName', 'VARCHAR2(255)'),
     DBCol('command', 'VARCHAR2(255)'),
     DBCol('ru_utime', 'NUMBER NOT NULL'),
     DBCol('ru_stime', 'NUMBER NOT NULL'),
     DBCol('ru_minflt', 'NUMBER'),
     DBCol('ru_majflt', 'NUMBER'),
     DBCol('ru_nswap', 'NUMBER'),
     DBCol('dependCond', 'VARCHAR2(255)'),
     DBCol('mailUser', 'VARCHAR2(255)'),
     DBCol('projectName', 'VARCHAR2(255)'),
     DBCol('exitStatus', 'NUMBER(5)'),
     DBCol('maxNumProcessors', 'NUMBER'),
     DBCol('loginShell', 'VARCHAR2(255)'),
     # MAX_JOB_ARRAY_SIZE can't be more than 2147483646 (10 digits), so I'll
     # naively assume you can't have an index bigger than that.
     DBCol('idx', 'NUMBER(10) NOT NULL'),
     DBCol('maxRMem', 'NUMBER NOT NULL'),
     DBCol('maxRSwap', 'NUMBER NOT NULL'),
     DBCol('exitInfo', 'NUMBER'),
     DBCol('chargedSAAP', 'VARCHAR2(255)'),
     DBCol('app', 'VARCHAR2(255)'),
     DBCol('runtimeEstimation', 'NUMBER'),
     DBCol('published', 'DATE NOT NULL', ots, dftval=0),
    ),
    # I don't want to include ABSOLUTE_TIME, which should be computed
    # on the fly, not hard-stored. Keeps things legible for everyone.
    pk=['jobId', 'idx', 'eventTime'],
    idxs=[['published', 'eventTime'],
          ['userName'],
          ['eventTime', 'startTime', 'queue'],
          ['queue', 'eventTime', 'startTime'],
          ['startTime', 'eventTime', 'queue'],  # cpuhours.py: started time
          ['submitTime', 'startTime', 'queue'], # cpuhours.py: waiting time
         ]
)

OLDLOCTAB = DBTab('lsf_acc',
    (
     DBCol('event_type', 'VARCHAR2(255) NOT NULL', src='eventType'),
     DBCol('version_number', 'VARCHAR2(255) NOT NULL', src='version'),
     DBCol('event_time', 'DATE NOT NULL', offsetOts, src='eventTime'),
     DBCol('jobid', 'NUMBER(10) NOT NULL', src='jobId'),
     DBCol('userid', 'NUMBER(7) NOT NULL', src='userId'),
     DBCol('options', 'NUMBER(10)'),
     DBCol('submit_time', 'DATE NOT NULL', offsetOts, src='submitTime'),
     DBCol('start_time', 'DATE NOT NULL', offsetOts, src='startTime'),
     DBCol('username', 'VARCHAR2(255) NOT NULL', src='userName'),
     DBCol('queue', 'VARCHAR2(255) NOT NULL'),
     DBCol('res_req', 'VARCHAR2(255)', lchop, src='resReq'),
     DBCol('depend_cond', 'VARCHAR2(255)', lchop, src='dependCond'),
     DBCol('exec_hosts', 'VARCHAR2(255)', catTrim, src='execHosts'),
     DBCol('job_status', 'NUMBER(5) NOT NULL', src='jStatus'),
     DBCol('host_factor', 'NUMBER(5,2) NOT NULL',
           lambda v: round(v, 2), src='hostFactor'),
     DBCol('job_name', 'VARCHAR2(255)', rchop, src='jobName'),
     # Annoying to compare and not very important anyway:
     # DBCol('command', 'VARCHAR2(255)', lchop),
     DBCol('ru_utime', 'NUMBER NOT NULL'),
     DBCol('ru_stime', 'NUMBER NOT NULL'),
     DBCol('ru_minflt', 'NUMBER', overflow),
     DBCol('ru_magflt', 'NUMBER', src='ru_majflt'),
     DBCol('ru_nswap', 'NUMBER'),
     DBCol('mail_user', 'VARCHAR2(255)', src='mailUser'),
     DBCol('project_name', 'VARCHAR2(255)', src='projectName'),
     DBCol('exit_status', 'NUMBER(5)', src='exitStatus'),
     DBCol('idx', 'NUMBER(10) NOT NULL'),
     DBCol('max_r_mem', 'NUMBER NOT NULL', src='maxRMem'),
     DBCol('max_r_swap', 'NUMBER NOT NULL', src='maxRSwap'),
    ),
    pk=['event_time', 'jobid', 'idx'],
)

# Stripped down CE table to only what we need because some of the fields
# we used to use are not available in the BLAH accounting files. Kept in
# addition whichever field is needed in the new APEL message format. The
# order is that of the message format. The columns are named after what they
# would have been called in the original CE DB.
CETAB = DBTab('ce',
    (
     # To handle jobid cycles (used to be eventTime)
     DBCol('timestamp', 'DATE NOT NULL', ots), 
     # SubmitHost APEL field (used to be ceId -- mind the case)
     DBCol('ceID', 'VARCHAR(256)'), 
     # For the join (used to be lrmsJobId)
     DBCol('lrmsID', 'NUMBER(10)'), 
     # For FQANs (used to be attribute)
     DBCol('userFQAN', 'VARCHAR(1023)'), # Once came across a 410 characters
                                         # FQAN rather early on
    ),
    pk=['timestamp', 'lrmsID'],
    idxs=[['lrmsId']]
)

TABS = dict([(LOCALTAB.name, LOCALTAB), (CETAB.name, CETAB)])

def daemonise(logger, pidfile):
    # http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python

    # Fork
    try:
        pid = os.fork()
        if pid > 0:
            return pid
    except OSError, e:
        logger.error("Couldn't fork: %s" % e)
        raise DaemonError()

    # Grow wings
    os.chdir('/')
    os.setsid() # Create new session, **with no controlling terminal**
    os.umask(0)

    # Fork again
    try:
        pid = os.fork()
        if pid > 0:
            return pid
    except OSError, e:
        logger.error("Couldn't refork: %s" % e)
        raise DaemonError()

    # Close taps
    sys.stdout.flush()
    sys.stderr.flush()
    stdin = file('/dev/null', 'r')
    stdout = file('/dev/null', 'a+')
    stderr = file('/dev/null', 'a+')
    os.dup2(stdin.fileno(), sys.stdin.fileno())
    os.dup2(stdout.fileno(), sys.stdout.fileno())
    os.dup2(stderr.fileno(), sys.stderr.fileno())

    # Write PID file
    try:
        f = open(pidfile, 'w')
        f.write('%d\n' % os.getpid())
        f.close()
    except IOError, e:
        logger.error("Couldn't write PID file: %s" % e)
        raise DaemonError()

def insert(logger, tab, recs, connection, insertc, errorc, 
           heartbeat=datetime.today(), heartbeatdelta=HBDELTA, 
           slice=None):
    '''
    Insert new rows into database.
    
    Expects a logger, a table template, an iterable lsb_geteventrec instance,
    an opened Oracle DB connection, an integer number of successful insertions,
    an integer number of errors, optionally a last heartbeat time, optionally
    a heartbeat period and optionally a DB column slice (useful for debugging).

    Returns the number of successful inserts, the number of errors and the
    last heartbeat.
    '''
    cursor = connection.cursor()

    for rec in recs:
        # Evaluate against actual value to see what we're up against
        try:
            for c in tab[:slice]:
                c.eval(rec)

            fmt = 'INSERT INTO %s VALUES (%s)'
            stmt = fmt % (tab, ', '.join([c.param for c in tab[:slice]]))

            l = filter(lambda v: v is not None, [c.val for c in tab[:slice]])

            cursor.execute(stmt, l)
            insertc += 1
        except cx_Oracle.DatabaseError, e:
            error, = e.args
            if error.code == 1: # ORA-00001: unique constraint
                if errorc % LOGBUNCH == 0:
                    logger.warning(INSERTERR % str(e)[:-1])
                    fmt = "Next %d duplicates won't be reported"
                    logger.warning(fmt % LOGBUNCH)
                    errorc = 0
            else:
                # I don't know what to make of that: just log but don't
                # reraise because I don't expect anyone to catch this and I
                # don't want the script to stop.
                logger.error(INSERTERR % str(e)[:-1])

                #for c in tab[:slice]:
                #    print "%s %s '%s'" % (c.col, c.type, str(c.val)[:60])
                #print
            errorc += 1
        except KeyError, e:
            # When you miss out keys when reading unflushed BLAH files
            # (Shouldn't happen any more, though)
            logger.error("%s: probably unflushed record for %s" % (e, rec))
        except Exception, e:
            logger.error(INSERTERR % e)
            errorc += 1

    try:
        connection.commit()
    except Exception, e:
        logger.error(COMMITERR % e)

    t = datetime.today()
    if t - heartbeat > timedelta(minutes=heartbeatdelta):
        fmt = "Inserted %d records in the last %d minutes"
        logger.info(fmt % (insertc, heartbeatdelta))
        heartbeat = datetime.today()
        insertc = 0
    return insertc, errorc, heartbeat

def gethosts(connection, table, clr, subclr=None):
    '''
    List CE hosts

    Expects:
    - a file with a user/passwd@dsn-formatted connection string
    - the table to get the host information from
    - the name of cluster string
    - optionally, the name of subcluster string

    Returns a list of host strings
    '''
    cursor = connection.cursor()
    select = 'SELECT hostname FROM %s' % table
    where = 'WHERE clustername = :clr'
    if subclr is None:
        cursor.execute('%s %s' % (select, where), (clr,))
    else:
        subclrcnd = "AND clustersubname = :subclr"
        cursor.execute('%s %s %s' % (select, where, subclrcnd), (clr,subclr))

    return [clrname for clrname, in cursor] # Yes, 'clrname,' to unpack tuple

def parsetype(type):
    '''
    Parse a DB type.

    Expects a fully-qualified DB type string (e.g. 'VARCHAR2(255) NOT NULL')
    returns a tuple made of:
    - the base type (e.g. VARCHAR2);
    - the length (e.g. 255) or None when not applicable;
    - a nullable boolean flag.
    '''

    m = TYPERE.match(type)
    type = m.group('type')

    # Only for VARCHAR2s
    if m.group('len'): # Inline if-else doesn't work on Python 2.4
        len = int(m.group('len'))
    else:
        len = None
    nullable = m.group('notnull') is None # Only for DATEs

    return type, len, nullable

def shortid(cols, prefix):
    '''
    Return a shortened Oracle identifier if needs be
    '''
    d = ORAIDLIM - len(prefix) - len('_'.join(cols))
    if d < 0:
        return prefix + '_'.join([c[:d / len(cols)] for c in cols])
    else:
        return prefix + '_'.join(cols)

def createstmts(tab, onlyidxs=False, noidxs=False, name=None, slice=None,
                idxspace=None, partition=None):
    '''
    Create string for table and index CREATE statements.

    Expects a DBTab instance, optionally a flag to only add indices, optionally
    a flag to ignore indices, optionally an override name for the table,
    optionally a DB column slice (useful for debugging) and optionally a
    specific table space to store the indices in.

    Return string for parametrised CREATE statements.
    
    '''
    stmts = []

    if name:
        tab.name = name

    # Build statement
    if not onlyidxs:
        fmt = 'CREATE TABLE %s (%s)'
        l = ['%s %s' % (c.col, c.type) for c in tab[:slice]]

        if partition == None:
            partclause = ""
        else:
            col, t = partition

            d = date.fromtimestamp(t)
            partclause = " PARTITION BY RANGE(%s)" % col
            partclause += " (PARTITION %s%d VALUES LESS THAN" % (name, t)
            # Can't do it with parameters it seems
            partclause += " (TO_DATE('%s', 'YYYY/MM/DD')))" % \
                d.strftime('%Y-%m-%d')
        stmts.append(fmt % (tab, ', '.join(l)) + partclause)

    # Any indices?
    if not noidxs:
        # Primary key
        if len(tab.pk) > 0:
            alter = 'ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s) '
            if idxspace == None:
                stmts.append(alter % (tab, shortid(tab.pk, 'pk_%s_' % tab),
                             ', '.join(tab.pk)))
            else:
                using = 'USING INDEX TABLESPACE %s'
                stmts.append((alter + using) % \
                             (tab, shortid(tab.pk, 'pk_%s_' % tab),
                              ', '.join(tab.pk), idxspace))

        # Other indices
        for i in tab.idxs:
            fmt = 'CREATE INDEX %s ON %s (%s) '
            if idxspace == None:
                stmts.append(fmt % (shortid(i, 'idx_%s_' % tab), tab,
                                    ', '.join(i)))
            else:
                space = 'TABLESPACE %s'
                stmts.append((fmt + space) % (shortid(i, 'idx_%s_' % tab), tab,
                                              ', '.join(i), idxspace))

    return stmts

def insertstmt(tab):
    '''
    Return parameterised INSERT statement query based on a DBTab instance
    passed as only parameter.
    '''
    fmt = 'INSERT INTO %s VALUES (%s)'
    return fmt % (tab, ', '.join([c.param for c in tab]))

def insertexec(cursor, stmt, cols, rec):
    '''
    Execute event record INSERT statement.

    Expects a cursor object, an INSERT statement string as returned by 
    insertstmt(), a DBCol sequence and an event record iterator. Doesn't
    return anything.
    '''
    cursor.execute(stmt, [c.eval(rec) for c in cols])

# FIXME To be merged with the other connect() function, maybe?
def myconnect(logger, connfile, host):
    '''
    Connect to CE database.
    
    Expects a file name string containing a connection string a la
    username/password@db. Returns a connection object.
    '''

    import MySQLdb

    # Parse supplied credentials file
    try:
        f = open(connfile)
        m = re.search(RE, f.readline())
        f.close()
        if m:
            username = m.group('username')
            password = m.group('password')
            db = m.group('dsn') # Not really dsn, in MySQL parlance, but well...
            return MySQLdb.connect(host, username, password, db)
        else:
            msg = "Wrong conn str format: try user/passwd@db"
            logger.error(msg)
            raise AcctDBError(msg)
    except IOError, (errno, strerr):
        msg = "Couldn't open connection file: %s" % strerr
        raise AcctDBError(msg)
    except MySQLdb.OperationalError, (errno, strerr):
        raise CEDBError(strerr)

def connect(logger, connfile):
    '''
    Connect to database.
    
    Expects a file name string containing a connection string a la
    username/password@dsn. Returns a connection object.
    '''
    try:
        # Use supplied credentials file
        logger.info("Reading DB connection file: %s" % connfile)
        f = open(connfile)
        m = re.search(RE, f.readline())
        f.close()
        if m:
            username = m.group('username')
            password = m.group('password')
            dsn = m.group('dsn')
            logger.info("Connecting to custom DSN: %s" % dsn)
            return cx_Oracle.connect(username, password, dsn)
        else:
            msg = "Wrong conn str format: try user/passwd@dsn"
            logger.error(msg)
            raise AcctDBError(msg)
    except IOError, (errno, strerr):
        msg = "Couldn't open connection file: %s" % strerr
        logger.error(msg)
        raise AcctDBError(msg)
    except cx_Oracle.DatabaseError, strerr:
        msg = "%s" % strerr
        logger.error(msg)
        raise AcctDBError(msg)

def accounting(logger, acctfile):
    '''
    Set up PyLSF and return accounting file name.

    Expects a logger and an accounting log file name. Returns the accounting
    log file name that has been decided upon (for book keeping purposes).
    '''
    import pylsf

    lsb = pylsf.lsb_init("pylsf-lsb.acct")
    if lsb == -1:
        #logging.warning("lsb_init() returned -1 -- Fishy?")
        pass

    if acctfile:
        # Use suggested accounting file
        logger.info("Opening custom acct file: %s" % acctfile)
    else:
        # Try default accounting file
        acctfile = DFTACCT
        logger.info("No accounting file suggested")
        logger.info("Opening default acct file: %s" % acctfile)

    return acctfile

def parse(fileobj, evthdl=None):
    '''
    Read new lines coming in fileobj, parse fields we're interested in
    publishing and order them as the DB expect them before yielding them in
    a list.
    '''

    # Skip what we've already processed
    if evthdl is not None:
        for _ in range(evthdl.offset): 
            # Can't rely on anything like self.insert + self.errorc because
            # self.insert is periodically reset for logging purposes.
            # It's worth noting self.offset is incremented within the
            # generator itself.
            fileobj.next()

    while True: # Not a for loop because we need to keep newlines
        l = fileobj.next()
        try:
            if l[-1] == '\n': # If it's a whole line...
                # ... proceed as usual in yielding the resulting dictionary
                fields = {}
                for f in RERECORD.findall(evthdl.buf + l): # For each field
                    k, v = f.split('=', 1)
                    if k == USERFQAN: 
                        # The userFQAN fields deserves some special treatment
                        # as there can be several of these and they have to
                        # be listed
                        if USERFQAN in fields:
                            fields[USERFQAN] += ' ' + v
                        else:
                            fields[USERFQAN] = v
                    elif k == TIMESTAMP: 
                        # The timestamp field also deserves some special
                        # treatment because it needs parsing
                        fields[k] = time.mktime(time.strptime(v, TFMT))
                    elif k == LRMSID:
                        # The lrmsID also also deserves some special treatment
                        # as I need it to be a real int. (Before you know it,
                        # everything will need some special treatment.)
                        fields[k] = int(v)
                    else:
                        fields[k] = v

                # Yield the resulting dictionary
                if evthdl is not None:
                    evthdl.offset += 1
                yield fields

                evthdl.buf = ''
            else: # If it's not a whole line...
                # ... buffer it for later on
                evthdl.buf = l
        except StopIteration:
            pass
