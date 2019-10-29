#!/usr/bin/env python
"""call request listener sid list from all listeners given in config file
   to generate discovery array for oradb.lld
   config file csv format is:
   'site;[cluster];alert_group;protocol;[user];[password];[password_enc];machine[,]...'
   site         - somesite
   cluster      - in case of RAC
   alert_group
   protocol     - ssh or psr
   user         - optional for ssh
   password     - plain text form of psr password (removed during encryption)
   password_enc - encrypted form of psr password
   machine[s]   - list of cluster members or single machine name

   run lsnrctl status on all machines and form the oradb.lld array
   """

# should work with python 2.7 and 3
from __future__ import print_function

import base64
import csv
import json
import os
import platform
import pwd
import shutil
import subprocess
import sys
from argparse import ArgumentParser
from tempfile import NamedTemporaryFile


def decomment(csvfile):
    for row in csvfile:
        raw = row.split('#')[0].strip()

        if raw: yield raw

def get_config(filename, _me):
    """read the specified configuration file"""

    if not os.path.exists(filename):
        raise ValueError("Configfile " + filename + " does not exist")

    lld_array = []
    with open(filename, 'r') as csvfile:
        reader = csv.DictReader(decomment(csvfile), delimiter=';')

        zbxnames = [ s.replace('{','{#') for s in reader.fieldnames ]

        for row in reader:
            _e = {}

            for col, zbxcol in zip(reader.fieldnames, zbxnames):
                if row[col]:
                    _e.update({zbxcol: row[col]})
            lld_array.append(_e)

    return lld_array

def main():
    """the entry point"""
    _me = os.path.splitext(os.path.basename(__file__))[0]
    _output = _me + ".lld"

    _parser = ArgumentParser()
    _parser.add_argument("-c", "--cfile", dest="configfile", default=_me+".csv",
                         help="Configuration file", metavar="FILE", required=False)
    _parser.add_argument("-v", "--verbosity", action="count", default=0,
                         help="increase output verbosity")
    _parser.add_argument("-l", "--lld_key", action="store", default="oradb.lld",
                         help="key to use for zabbix_host lld")
    _parser.add_argument("-z", "--zabbix_host", action="store",
                         help="zabbix hostname that has the oradb.lld rule")
    _parser.add_argument("-s", "--server", action="store", default="localhost",
                         help="zabbix server or proxy name")
    _parser.add_argument("-p", "--port", action="store", default="10051",
                         help="port of zabbix server of proxy to send to")
    _args = _parser.parse_args()

    lld_array = get_config(_args.configfile, _me)

    if _args.verbosity:
        print(_args)

    OUTPUT = _me + ".lld"

    if _args.zabbix_host:
        if _args.verbosity:
            print("Writing to {}".format(OUTPUT))
        array = '\"' + str(_args.zabbix_host) + '\" \"' + _args.lld_key + '\"' \
            ' ' + '{\"data\":' + json.dumps(lld_array) + '}'
        F = open(OUTPUT, "w")
        F.write(array)
        F.close()
        CMD = "zabbix_sender -z {} -p {} -i {} -r  -vv".format(
            _args.server, _args.port, OUTPUT)
        os.system(CMD)
    else:
        print('{\"data\":' + json.dumps(lld_array) + '}')

if __name__ == "__main__":

    main()
