#! /usr/bin/env python

import optparse
import sys
import common
import logging

def main():
    # Read arguments
    p = optparse.OptionParser()
    help = "user/passwd@dsn-formatted database connection file path"
    p.add_option("-c", "--connfile", help=help)
    help = "list available table templates"
    p.add_option("-l", "--listtabs", action='store_true', help=help)
    help = "create table according to template TEMPLATE"
    p.add_option("-t", "--template", help=help)
    help = "override table name"
    p.add_option("-n", "--name", help=help)
    help = "number of columns to handle (defaults to %d)" % \
        len(common.LOCALTAB)
    p.add_option("-s", "--slice", type='int', help=help)
    help="""don't create table but only add primary key and indices (takes \
the table referred to with -n into account if specified)"""
    p.add_option("-i", "--onlyindices", action='store_true', help=help)
    help="disable indices"
    p.add_option("-j", "--noindices", action='store_true', help=help)
    help="don't do anything, only SQL-print what would be done"
    p.add_option("-d", "--dryrun", action='store_true', help=help)
    p.add_option("-u", "--tablespace", help="index table space")
    options, args = p.parse_args()

    # Set up logging
    #fmt = "%(asctime)s %(name)s: %(levelname)s %(message)s"
    #logging.basicConfig(level=logging.INFO, format=fmt)
    logger = logging.getLogger(common.LOGGER)

    if options.listtabs:
        for t in common.TABS:
            print t
        return
    
    if options.template:
        try:
            stmts = common.createstmts(common.TABS[options.template],
                                       options.onlyindices,
                                       options.noindices,
                                       options.name, options.slice,
                                       options.tablespace)

            connection = common.connect(logger, options.connfile)
            cursor = connection.cursor()
            for s in stmts:
                print s
                if not options.dryrun:
                    cursor.execute(s)
        except KeyError:
            print >>sys.stderr, "%s: No such table template" % options.template
    else:
        p.print_usage()
        return

if __name__ == '__main__':
    main()
