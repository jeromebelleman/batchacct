#! /usr/bin/env python

import optparse, ConfigParser
import logging
import datetime
import sys
import os, os.path
import string
import time
from itertools import izip, islice
# Let's not look at the exit status or info: guess a job can run, consume & fail
from common import STTCOND, CPUCOND
import common

EPOCH = datetime.datetime(1970, 1, 1, 1, 0, 0)
CFG = '.cpuhours.cfg'
HS = 3.9 # HEPSPEC06
CONNFILE = '~/.batchacct/connection'
BINNING = 'DDD'
TRANS = string.maketrans('/', '_')
BARW = 5. # Chart bar width
COLOUR = 'r'
WEEKS = 2
BEGIN = time.mktime((datetime.date.today() -
                     datetime.timedelta(weeks=WEEKS)).timetuple())
END = time.mktime(datetime.date.today().timetuple())

PLANCOLS = ['id', 'operation', 'options', 'optimizer', 'cost', 'cardinality',
            'bytes', 'cpu_cost', 'io_cost', 'time']

DELTA = {'HH24': datetime.timedelta(hours=1),
         'DDD': datetime.timedelta(days=1),
         'WW': datetime.timedelta(weeks=1),
         'MI': datetime.timedelta(minutes=1),
        }

def fillgaps(xs, ys, binning):
    '''
    Look for missing bins where no value has been found and set 0 to them.

    Expect xs and ys values as well as the Oracle (MI, HH24, DDD, WW ...) and
    fill in the gaps in xs and ys in place.
    '''

    i = 1
    try:
        while True:
            if xs[i] - xs[i - 1] > DELTA[binning]:
                xs.insert(i, xs[i - 1] + DELTA[binning])
                ys.insert(i, 0)
            else:
                i += 1
    except IndexError:
        pass

def labels(plt, count, walltime, waiting, cumuwaiting, started, nonorm):
    '''
    Set y label to plot passed as first argument according to the options
    passed as the other arguments.
    '''

    if count:
        plt.ylabel('number of finished jobs')
    elif started:
        plt.ylabel('number of started jobs')
    elif walltime:
        plt.ylabel('walltime (days)')
    elif waiting:
        plt.ylabel('waiting time (days)')
    elif cumuwaiting:
        plt.ylabel('cumulative waiting time (days)')
    elif nonorm:
        plt.ylabel('CPU time (days)')
    else:
        plt.ylabel('normalised HEPSPEC06 (days)')
    plt.axis(ymin=0)

def mktitle(title, what, users, fromhosts):
    '''
    Return suitable title based on what, users and fromhosts arguments if the
    title passed as first is None.
    '''

    crits = '-'.join([e for e in (what, users, fromhosts) if e != None])

    if title != None:
        return title
    elif crits:
        return crits
    else:
        return 'everything'

def mkplan(cursor, stmt):
    '''
    Print plan for statement passed as second argument.
    '''

    latest = 'SELECT MAX(plan_id) FROM plan_table'
    explain = 'SELECT %s FROM plan_table WHERE plan_id = (%s)' % \
        (', '.join(PLANCOLS), latest)
    cursor.execute('EXPLAIN PLAN FOR ' + stmt)
    cursor.execute(explain)
    common.ftab(list(cursor), PLANCOLS)

# FIXME Could probably make mkcritcond and mkcond into a single,
# more generic function.

def mkcritcond(crits):
    '''
    Return criteria statement and parameters from criterion list passed
    as argument.
    '''

    # Queues or groups (which may be a comma-separated list or a file with
    # a commma-separated list)
    if crits == None:
        critcond, crits = '', []
    else:
        if os.path.isfile(crits):
            f = open(crits)
            crits = f.read().split(',')
            f.close()
        else:
            crits = crits.split(',')

        # labels = ('chargedSAAP' if c[0] == '/' else 'queue' for c in crits)
        labels = []
        for c in crits:
            if c[0] == '/':
                labels.append('chargedSAAP')
            else:
                labels.append('queue')

        # cops = ('LIKE' if '%' in c or c[0] == '/' else '=' for c in crits)
        cops = []
        for c in crits:
            if '%' in c or c[0] == '/':
                cops.append('LIKE')
            else:
                cops.append('=')

        critcond = " AND (%s)" % ' OR '.join(['%s %s :c%d' % (l, o, i)
                                             for l, (i, o)
                                             in izip(labels, enumerate(cops))])
    return critcond, crits

def mkcond(items, column):
    '''
    Return statement and parameters from item list and column name passed as
    argument.
    '''

    # Items (which may be a comma-separated list or a file with a
    # commma-separated list)
    if items == None:
        itemcond, items = '', []
    else:
        if os.path.isfile(items):
            f = open(items)
            items = f.read().split(',')
            f.close()
        else:
            items = items.split(',')

        fops = []
        for h in items:
            if '%' in h:
                fops.append('LIKE')
            else:
                fops.append('=')
                
        itemcond = " AND (%s)" % ' OR '.join(['%s %s :f%d' % (column, op, i)
                                              for i, op in enumerate(fops)])
    return itemcond, items

def waitdistdbread(logger, connfile, table, begin, end, crits, users, hosts,
                   title, plan, norm):
    '''
    Connect to DB, run query and store x values into a list which is returned.

    Specific to getting data for plotting a histogram of the waiting
    distribution, i.e. dbread() isn't really suitable for doing it (not
    least because we don't have time on the x axis, we have walltimes).
    '''

    # Connect
    c = common.connect(logger, os.path.expanduser(connfile))
    cursor = c.cursor()

    # Queues or groups
    critcond, crits = mkcritcond(crits)

    # Submit hosts
    hostcond, hosts = mkcond(hosts, 'fromHost')

    # Users
    usercond, users = mkcond(users, 'userName')

    # Normalisation factor
    if norm == None:
        factor = ''
    else:
        factor = ' * hostFactor * %s' % norm

    # Query
    # Difference is already in days by virtue of Oracle
    sel = "SELECT (eventTime - startTime) * 24 %s" % factor
    tab = "FROM %s" % table

    # Time condition is compulsory and there are default values anyway
    timecond = "WHERE eventTime BETWEEN :begin AND :end"
    span = [datetime.date.fromtimestamp(begin),
            datetime.date.fromtimestamp(end)]

    # Statement
    stmt = '%s %s %s AND %s AND %s %s' % \
        (sel, tab, timecond, STTCOND, CPUCOND, critcond + hostcond + usercond)
    params = span + [EPOCH] + crits + hosts + users

    print "Querying..."
    t = time.time()
    if plan:
        mkplan(cursor, stmt)

    cursor.execute(stmt, params)

    # Run query and store values
    f = open(title.translate(TRANS) + '.data', 'w')
    xs = []
    for x, in cursor:
        print >>f, '%f' % x
        xs.append(x)
    print "Queried in %f s" % (time.time() - t)
    f.close()
    return xs

def dbread(logger, connfile, table, begin, end, crits, users, hosts, title,
           binning, count, walltime, waiting, cumuwaiting, started, plan, norm):
    '''
    Connect to DB, run query and store x and y values into two separate
    lists returned in two separate tuples.
    '''

    # Connect
    c = common.connect(logger, os.path.expanduser(connfile))
    cursor = c.cursor()

    # Queues or groups
    critcond, crits = mkcritcond(crits)

    # Submit hosts
    hostcond, hosts = mkcond(hosts, 'fromHost')

    # Users
    usercond, users = mkcond(users, 'userName')

    # Group
    if started:
        grp = 'startTime'
    else:
        grp = 'eventTime'

    # Normalisation factor
    if norm == None:
        factor = ''
    else:
        factor = ' * hostFactor * %s' % norm

    # SELECT
    if count or started:
        col = "COUNT(*)"
    elif walltime:
        # Difference is already in days by virtue of Oracle, and that's what
        # we want (i.e. no / 60 / 60 / 24)
        col = "SUM((eventTime - startTime) %s)" % factor
    elif waiting or cumuwaiting:
        col = "SUM((startTime - submitTime) %s)" % factor
    else:
        col = "SUM((ru_stime + ru_utime) %s / 60 / 60 / 24)" % factor

    # Query
    # DDD if we had wanted HEPSPEC06 in days
    sel = "SELECT TRUNC(%s, '%s'), %s" % (grp, binning, col)
    tab = "FROM %s" % table

    # Time condition is compulsory and there are default values anyway
    if started:
        timecond = "WHERE startTime BETWEEN :begin AND :end"
    else:
        timecond = "WHERE eventTime BETWEEN :begin AND :end"
    span = [datetime.date.fromtimestamp(begin),
            datetime.date.fromtimestamp(end)]

    grpexpr = "GROUP BY TRUNC(%s, '%s')" % (grp, binning)

    # Statement
    stmt = '%s %s %s AND %s AND %s %s %s' % \
        (sel, tab, timecond, STTCOND, CPUCOND,
         critcond + hostcond + usercond, grpexpr)
    params = span + [EPOCH] + crits + hosts + users

    print "Querying..."
    t = time.time()
    if plan:
        mkplan(cursor, stmt)

    cursor.execute(stmt, params)

    # Run query and store values
    f = open(title.translate(TRANS) + '.data', 'w')
    xs, ys = [], []
    # FIXME What's this i here for?
    for i, (x, y) in enumerate(sorted(cursor, key=lambda r: r[0])):
        print >>f, '%d %f' % (time.mktime(x.timetuple()), y)
        xs.append(x)
        if cumuwaiting:
            try:
                ys.append(ys[-1] + y)
            except IndexError: # First time
                ys.append(y)
        else:
            ys.append(y)
    print "Queried in %f s" % (time.time() - t)
    f.close()

    return xs, ys

def fileread(title):
    '''
    Read data file which has been generated by a previous run of this script
    to build and return a tuple of x and y lists.
    '''

    f = open(title.translate(TRANS) + '.data', 'r')
    xs, ys = [], []
    for l in f:
        x, y = l.split()
        xs.append(datetime.date.fromtimestamp(float(x)))
        ys.append(float(y))
    f.close()

    return xs, ys

def main():
    # Args
    desc = "Plot CPU time data, by default HEPSPEC06-normalised."
    p = optparse.OptionParser(description=desc)
    help = 'user/passwd@dsn-formatted DB connection file (defaults to %s)' % \
        CONNFILE
    p.add_option('-c', '--connfile', default=CONNFILE, help=help)
    help = 'begin date (defaults to %d, i.e. %d weeks ago)' % (BEGIN, WEEKS)
    p.add_option("-b", "--begin", type='int', default=BEGIN, help=help)
    help = 'end date (defaults to %d, i.e. today)' % END
    p.add_option("-e", "--end", type='int', default=END, help=help)
    help = "comma-sep'd list or file of comma-sep'd list of queues or groups"
    help += ' (non-stacked, defaults to all, supports %-wildcards)'
    p.add_option("-x", "--what", help=help)
    help = "comma-sep'd list or file of comma-sep'd list of users"
    help += ' (non-stacked, defaults to all, supports %-wildcards)'
    p.add_option("-y", "--users", help=help)
    help = "comma-sep'd list or file of comma-sep'd list of submit hosts"
    help += ' (non-stacked, defaults to all, supports %-wildcards)'
    p.add_option("-m", "--fromhosts", help=help)
    help = 'plot title (defaults to the queried queues/groups)'
    p.add_option("-t", "--title", help=help)
    help = 'plot colour (defaults to %s)' % COLOUR
    p.add_option("-k", "--colour", default=COLOUR, help=help)
    help = "plot bars instead of line (non-stacked, against missing zeros)"
    p.add_option("-i", "--bar", action='store_true', help=help)
    help = 'log scale (non-stacked)'
    p.add_option("-l", "--log", action='store_true', help=help)
    help = "don't read from DB, read from titled file"
    p.add_option("-f", "--file", action='store_true', default=False, help=help)
    help='table (defaults to %s)' % common.LOCALTAB
    p.add_option("-r", "--table", default=common.LOCALTAB, help=help)
    help = "stack plots for comma-sep'd list of data title (file without ext)"
    p.add_option("-s", "--stack", help=help)
    help = 'plot finished job count instead of CPU time'
    p.add_option("-n", "--count", action='store_true', help=help)
    help = 'plot started job count instead of CPU time'
    p.add_option("-o", "--started", action='store_true', help=help)
    help = 'plot walltime instead of CPU time'
    p.add_option("-w", "--walltime", action='store_true', help=help)
    help = 'plot waiting time instead of CPU time'
    p.add_option("-v", "--waiting", action='store_true', help=help)
    help = 'plot cumulative waiting time instead of CPU time'
    p.add_option("-u", "--cumuwaiting", action='store_true', help=help)
    help = 'plot distribution of waiting time'
    p.add_option("-d", "--waitdist", action='store_true', help=help)
    help = 'cumulative distribution of waiting time in percent (only with -d)'
    p.add_option("-q", "--percent", action='store_true', help=help)
    help = 'binning (any of MI, HH24, DDD, WW, defaults to %s)' % BINNING
    # 'a' for aggregate
    p.add_option("-a", "--binning", default=BINNING, help=help)
    p.add_option("-p", "--plan", action='store_true', help='explain query plan')
    p.add_option("-z", "--nonorm", action='store_true', help="don't normalise")
    opts, args = p.parse_args()

    # Import later to avoid X errors when you only want to get the help menu
    import numpy as npy
    import matplotlib.pyplot as plt
    import matplotlib.cm as cm

    # Read configuration file
    cfg = ConfigParser.RawConfigParser({'factor': str(HS)})
    cfg.add_section('main')
    cfg.read([os.path.expanduser('~/' + CFG), CFG])

    # Logs
    fmt = '%(asctime)s %(levelname)s %(message)s'
    h = logging.FileHandler('/dev/null') # I'm such a brutal sort of person
    h.setFormatter(logging.Formatter(fmt))
    logger = logging.getLogger(common.LOGGER)
    logger.addHandler(h)
    logger.setLevel(logging.INFO)

    # Normalisation
    if opts.nonorm:
        norm = None
    else:
        norm = cfg.get('main', 'factor')

    # Stack only works with data files (not DB -- too heavy)
    if opts.stack is not None:
        # Collect all yss
        print "Loading data..."
        yss = []
        files = [f.strip() for f in opts.stack.split(',')]
        for f in files:
            xs, ys = fileread(f)
            yss.append(ys)
        yss = npy.cumsum(yss, axis=0)

        # Stack it all up
        print "Plotting..."
        fig = plt.figure()
        ax = fig.add_subplot(111)

        colours = cm.prism(npy.arange(0, 1, 1. / len(yss)))
        ax.fill_between(xs, yss[0], 0, facecolor=colours[0])
        for i, ys in enumerate(islice(yss, 1, None), 1):
            ax.fill_between(xs, yss[i - 1], yss[i], facecolor=colours[i])

        # Proxy artists
        rects = []
        for i, f in enumerate(files):
            rects.append(plt.Rectangle((0,0), 1, 1, fc=colours[i]))
        plt.legend(rects, files)

        labels(plt, opts.count, opts.walltime, opts.waiting, opts.cumuwaiting,
               opts.started, opts.nonorm)
        fig.autofmt_xdate()
        plt.title(opts.stack)
        plt.savefig(opts.stack + '-stacked.pdf')
    elif opts.waitdist:
        try:
            # Set a title
            title = mktitle(opts.title, opts.what, opts.users, opts.fromhosts)

            # Get data
            xs = waitdistdbread(logger, opts.connfile, opts.table, opts.begin,
                                opts.end, opts.what, opts.users, opts.fromhosts,
                                title, opts.plan, norm)

            # XXX What not use label()?

            # Plot histogram
            print "Plotting..."
            fig = plt.figure()
            ax1 = fig.add_subplot(111)
            for l in ax1.get_xticklabels():
                l.set_rotation(30)
            #n, bins, _ = ax1.hist(xs, log=opts.log, color=opts.colour)
            # Can't easily get round bins (e.g. exactly one hour) because
            # bins can't easily be of different size.
            binning = (DELTA[opts.binning].days * 24 * 60 * 60 + \
                DELTA[opts.binning].seconds) / 60. / 60.
            n, bins, _ = ax1.hist(xs, bins=max(xs) / binning, log=opts.log,
                color=opts.colour)
            plt.ylabel('number of jobs')
            plt.xlabel('hours')

            # Plot cumulated derivative
            ax2 = plt.twinx()
            if opts.percent:
                d = [float(v) / sum(n) * 100 for v in npy.cumsum(n)]
                plt.ylabel('cumulative derivative (%)')
            else:
                d = [float(v) for v in npy.cumsum(n)]
                plt.ylabel('cumulative derivative (absolute)')
            if opts.log:
                # Not 'log' because it misbehaves with plt.axis()
                plt.yscale('symlog')
            plt.plot(bins[1:], d)
            plt.axis(ymin=0)
            plt.xticks([round(b, 3) for b in bins])

            plt.title(title)
            plt.savefig(title.translate(TRANS) + '-waitdist')
        except common.AcctDBError, e:
            print >>sys.stderr, e
            return 1
    else:
        # Set a title
        title = mktitle(opts.title, opts.what, opts.users, opts.fromhosts)

        # Get data
        try:
            if opts.file:
                xs, ys = fileread(title)
            else:
                xs, ys = dbread(logger, opts.connfile, opts.table, opts.begin,
                                opts.end, opts.what, opts.users, opts.fromhosts,
                                title, opts.binning, opts.count, opts.walltime,
                                opts.waiting, opts.cumuwaiting, opts.started,
                                opts.plan, norm)
        except common.AcctDBError, e:
            print >>sys.stderr, e
            return 1

        # Fill in missing zeros if any
        fillgaps(xs, ys, opts.binning)

        # Plot
        print "Plotting..."
        fig = plt.figure()
        ax = fig.add_subplot(111)
        if opts.bar:
            ax.bar(xs, ys, color=opts.colour, width=BARW / len(ys),
                   linewidth=0, log=opts.log)
        else:
            if opts.log:
                ax.semilogy(xs, ys, opts.colour)
            else:
                ax.plot(xs, ys, opts.colour)
        fig.autofmt_xdate()

        plt.title(title)
        labels(plt, opts.count, opts.walltime, opts.waiting, opts.cumuwaiting,
               opts.started, opts.nonorm)

        if opts.count:
            plt.savefig(title.translate(TRANS) + '-count')
        elif opts.walltime:
            plt.savefig(title.translate(TRANS) + '-walltime')
        elif opts.waiting:
            plt.savefig(title.translate(TRANS) + '-waiting')
        elif opts.cumuwaiting:
            plt.savefig(title.translate(TRANS) + '-cumuwaiting')
        elif opts.started:
            plt.savefig(title.translate(TRANS) + '-started')
        else:
            plt.savefig(title.translate(TRANS))

if __name__ == '__main__':
    sys.exit(main())
