#! /usr/bin/env python

import datetime, time
import optparse
import logging
from itertools import izip
import common

LOGFILE = '/var/log/batchacct/batchacct-partition.log'
AHEAD = 3

def lastpartition(cursor, tab):
    '''
    Find out last partition UNIX timestamp from DB

    Expects:
    - a DB cursor
    - a table name
    
    Returns a UNIX timestamp integer
    '''
    select = "SELECT MAX(partition_name) FROM user_tab_partitions"
    where = "WHERE TABLE_NAME = :t"
    stmt = "%s %s" % (select, where)
    cursor.execute(stmt, [tab.upper()])

    return int(list(cursor)[0][0].split(tab.upper())[-1])

def createpartitions(tab, t, n=3):
    '''
    Build SQL ALTER statements to add the new partitions for each missing
    month to today and n months ahead of today.

    Expects:
    - a table name tab
    - a UNIX timestamp t which to add partitions from
    - an integer of months to go ahead of today

    Returns a list statement string list
    '''

    stmts, months = [], []

    # Delta between today and last partition in months
    then = datetime.date.fromtimestamp(t)
    today = datetime.date.today()
    d = today.month + today.year * 12 - (then.month + then.year * 12) + n

    # Range of (year, month) pairs in delta
    rge = ((then.year + m / 12, m % 12 + 1)
           for m in range(then.month, d + then.month))

    # Build statement for each missing month and n months ahead
    for y, m in rge:
        part = datetime.date(y, m, 1)
        # Build statement
        stmt = "ALTER TABLE %s ADD PARTITION %s%d VALUES LESS THAN" % \
            (tab, tab, time.mktime(part.timetuple()))
        stmt += " (TO_DATE('%s', 'YYYY/MM/DD'))" % part.strftime('%Y-%m-%d')
        stmts.append(stmt)
        months.append(part.strftime('%B %Y'))

    return stmts, months

def main():
    # Read arguments
    p = optparse.OptionParser()
    help = "user/passwd@dsn-formatted database connection file path"
    p.add_option("-c", "--connfile", help=help)
    help = "table name"
    p.add_option("-t", "--table", help=help)
    help="don't do anything, only SQL-print what would be done"
    p.add_option("-d", "--dryrun", action='store_true', help=help)
    help = "log file absolute path (defaults to %s)" % LOGFILE
    p.add_option("-l", "--logfile", help=help, default=LOGFILE)
    help = "how many months to plan ahead (defaults to %d)" % AHEAD
    p.add_option("-p", "--plan", help=help, type='int', default=AHEAD)
    options, args = p.parse_args()

    if None in (options.connfile, options.table):
        p.print_help()
        return 1

    # Set up logging
    h = logging.FileHandler(options.logfile)
    fmt = "%(asctime)s %(name)s: %(levelname)s %(message)s"
    h.setFormatter(logging.Formatter(fmt, common.LOGDATEFMT))
    logger = logging.getLogger(common.LOGGER)
    logger.addHandler(h)
    logger.setLevel(logging.INFO)

    # DB
    connection = common.connect(logger, options.connfile)
    cursor = connection.cursor()

    # Create partitions
    last = lastpartition(cursor, options.table)
    stmts, months = createpartitions(options.table.upper(), last, options.plan)
    for stmt, month in izip(stmts, months):
        logger.info("Adding partition for %s to %s" % (month, options.table))
        logger.info(stmt)
        if not options.dryrun:
            cursor.execute(stmt)
    logger.info("Done")

if __name__ == '__main__':
    main()
