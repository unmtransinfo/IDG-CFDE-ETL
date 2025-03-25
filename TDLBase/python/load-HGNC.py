#!/usr/bin/env python3
# Time-stamp: <2025-02-13 17:48:19 smathias>
"""Load HGNC annotations for targets into a TDLBase MySQL DB from downloaded TSV file.

Usage:
    load-HGNC.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-HGNC.py -h | --help

Options:
  -h --dbhost DBHOST   : MySQL database host name [default: localhost]
  -n --dbname DBNAME   : MySQL database name [default: tcrdev]
  -l --logfile LOGF    : set log file name
  -v --loglevel LOGL   : set logging level [default: 30]
                         50: CRITICAL
                         40: ERROR
                         30: WARNING
                         20: INFO
                         10: DEBUG
                          0: NOTSET
  -q --quiet           : set output verbosity to minimal level
  -d --debug           : turn on debugging output
  -? --help            : print this message and exit 
"""
__author__ = "Steve Mathias"
__email__ = "smathias@salud.unm.edu"
__org__ = "Translational Informatics Division, UNM School of Medicine"
__copyright__ = "Copyright 2025, Steve Mathias"
__license__ = "Creative Commons Attribution-NonCommercial (CC BY-NC)"
__version__ = "1.0.0"

import os,sys,time
from docopt import docopt
from TDLB.Adaptor import Adaptor
import logging
import csv
import slm_util_functions as slmf

PROGRAM = os.path.basename(sys.argv[0])
LOGDIR = f"../log/TDLBase/"
LOGFILE = f"{LOGDIR}/{PROGRAM}.log"
HGNC_TSV_FILE = '../data/HGNC/HGNC_20250213.tsv'

def load(args, dba, logger, logfile):
  line_ct = slmf.wcl(HGNC_TSV_FILE)
  if not args['--quiet']:
    print(f"\nProcessing {line_ct} lines in file {HGNC_TSV_FILE}")
  ct = 0
  hgnc_ct = 0
  chr_ct = 0
  sym_ct = 0
  symdiscr_ct = 0
  geneid_ct = 0
  geneiddiscr_ct = 0
  notfnd = set()
  tmark = set()
  db_err_ct = 0
  with open(HGNC_TSV_FILE, 'r') as ifh:
    tsvreader = csv.reader(ifh, delimiter='\t')
    for row in tsvreader:
      # 0: HGNC ID
      # 1: Approved symbol
      # 2: Approved name
      # 3: Status
      # 4: Chromosome
      # 5: NCBI Gene ID
      # 6: UniProt ID
      if ct == 0:
        header = row # header line
        ct += 1
        continue
      ct += 1
      slmf.update_progress(ct/line_ct)
      sym = row[1]
      if row[5] != '':
        geneid = int(row[5])
      else:
        geneid = None
      if row[6] != '':
        up = row[6]
      else:
        up = None
      tids = dba.find_target_ids({'sym': sym})
      if not tids and geneid:
        tids = dba.find_target_ids({'geneid': geneid})
      if not tids and up:
        tids = dba.find_target_ids({'uniprot': up})
      if up and not tids:
        notfnd.add(f"{sym}|{geneid}|{up}")
        logger.warning(f"No target found for {sym}|{geneid}|{up}")
        continue
      for tid in tids:
        if tid in tmark:
          continue  # we've already annotated this target w/ HGNC info
        target = dba.get_target(tid)
        # HGNC xref
        hgncid = row[0].replace('HGNC:', '')
        rv = dba.ins_xref({'target_id': tid, 'xtype': 'HGNC ID',
                           'value': hgncid})
        if rv:
          hgnc_ct += 1
        else:
          db_err_ct += 1
        # Add target.chr values
        rv = dba.do_update({'table': 'target', 'col': 'chr', 'id': tid, 'val': row[4]})
        if rv:
          chr_ct += 1
        else:
          db_err_ct += 1
        # Add missing syms
        if target['sym'] == None:
          rv = dba.do_update({'table': 'target', 'col': 'sym', 'id': tid, 'val': sym})
          if rv:
            logger.info("Inserted new sym {} for target {}|{}".format(sym, tid, target['uniprot']))
            sym_ct += 1
          else:
            db_err_ct += 1
        else:
          # Check for symbol discrepancies
          if target['sym'] != sym:
            logger.warning("Symbol discrepancy: UniProt's=%s, HGNC's=%s" % (target['sym'], sym))
            symdiscr_ct += 1
        if geneid:
          # Add missing geneids
          if target['geneid'] == None:
            rv = dba.do_update({'table': 'target', 'col': 'geneid', 'id': tid, 'val': geneid})
            if rv:
              logger.info("Inserted new geneid {} for target {}, {}".format(geneid, tid, target['uniprot']))
              geneid_ct += 1
            else:
              db_err_ct += 1
          else:
            # Check for geneid discrepancies
            if target['geneid'] != geneid:
              logger.warning("GeneID discrepancy: UniProt's={}, HGNC's={}".format(target['geneid'], geneid))
              geneiddiscr_ct += 1
        tmark.add(tid)
  print("Processed {} lines - {} targets annotated.".format(ct, len(tmark)))
  if notfnd:
    print("No target found for {} lines (with UniProts).".format(len(notfnd)))
  print(f"  Inserted {hgnc_ct} HGNC ID xrefs")
  print(f"  Updated {chr_ct} target.chr values.")
  print(f"  Inserted {sym_ct} new HGNC symbols")
  if symdiscr_ct > 0:
    print(f"WARNING: Found {symdiscr_ct} discrepant HGNC symbols. See logfile {logfile} for details")
  if geneid_ct > 0:
    print(f"  Inserted {geneid_ct} new NCBI Gene IDs")
  if geneiddiscr_ct > 0:
    print(f"WARNING: Found {geneiddiscr_ct} discrepant NCBI Gene IDs. See logfile {logfile} for details")
  if db_err_ct > 0:
    print(f"WARNING: {db_err_ct} DB errors occurred. See logfile {logfile} for details.")


if __name__ == '__main__':
  print("\n{} (v{}) [{}]:\n".format(PROGRAM, __version__, time.strftime("%c")))
  start_time = time.time()

  args = docopt(__doc__, version=__version__)
  if args['--debug']:
    print(f"\n[*DEBUG*] ARGS:\nargs\n")
  if args['--logfile']:
    logfile =  args['--logfile']
  else:
    logfile = LOGFILE
  loglevel = int(args['--loglevel'])
  logger = logging.getLogger(__name__)
  logger.setLevel(loglevel)
  if not args['--debug']:
    logger.propagate = False # turns off console logging
  fh = logging.FileHandler(LOGFILE)
  fmtr = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
  fh.setFormatter(fmtr)
  logger.addHandler(fh)

  dba_params = {'dbhost': args['--dbhost'], 'dbname': args['--dbname'], 'logger_name': __name__}
  dba = Adaptor(dba_params)
  dbi = dba.get_dbinfo()
  logger.info("Connected to TDLBase: {} (schema ver {}; data ver {})".format(args['--dbname'], dbi['schema_ver'], dbi['data_ver']))
  if not args['--quiet']:
    print("Connected to TDLBase:: {} (schema ver {}; data ver {})".format(args['--dbname'], dbi['schema_ver'], dbi['data_ver']))

  load(args, dba, logger, logfile)
    
  elapsed = time.time() - start_time
  print("\n{}: Done. Elapsed time: {}\n".format(PROGRAM, slmf.secs2str(elapsed)))
