#!/usr/bin/env python
# Time-stamp: <2025-02-13 18:25:41 smathias>
"""Load NCBI gi xrefs into a TDLBase MySQL DB from UniProt ID Mapping file.

Usage:
    load-GIs.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-GIs.py -? | --help

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
__author__    = "Steve Mathias"
__email__     = "smathias @salud.unm.edu"
__org__       = "Translational Informatics Division, UNM School of Medicine"
__copyright__ = "Copyright 2025, Steve Mathias"
__license__   = "Creative Commons Attribution-NonCommercial (CC BY-NC)"
__version__   = "1.0.0"

import os,sys,time
from docopt import docopt
from TDLB.Adaptor import Adaptor
import logging
import csv
from urllib.request import urlretrieve
import gzip
import slm_util_functions as slmf

PROGRAM = os.path.basename(sys.argv[0])
LOGDIR = f"../log/TDLBase/"
LOGFILE = f"{LOGDIR}/{PROGRAM}.log"
DOWNLOAD_DIR = '../data/UniProt/'
BASE_URL = 'ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping/by_organism/'
FILENAME = 'HUMAN_9606_idmapping_selected.tab.gz'

def download(args):
  gzfn = DOWNLOAD_DIR + FILENAME
  if os.path.exists(gzfn):
    os.remove(gzfn)
  fn = gzfn.replace('.gz', '')
  if os.path.exists(fn):
    os.remove(fn)
  if not args['--quiet']:
    print "\nDownloading ", BASE_URL + FILENAME
    print "         to ", gzfn
  urlretrieve(BASE_URL + FILENAME, gzfn)
  print "Uncompressing", gzfn
  ifh = gzip.open(gzfn, 'rb')
  ofh = open(fn, 'wb')
  ofh.write( ifh.read() )
  ifh.close()
  ofh.close()

def load(args, dba, logger. logfile):
  fn = (DOWNLOAD_DIR + FILENAME).replace('.gz', '')
  line_ct = slmf.wcl(infile)
  # ID Mappiing fields
  # 1. UniProtKB-AC
  # 2. UniProtKB-ID
  # 3. GeneID (EntrezGene)
  # 4. RefSeq
  # 5. GI
  # 6. PDB
  # 7. GO
  # 8. UniRef100
  # 9. UniRef90
  # 10. UniRef50
  # 11. UniParc
  # 12. PIR
  # 13. NCBI-taxon
  # 14. MIM
  # 15. UniGene
  # 16. PubMed
  # 17. EMBL
  # 18. EMBL-CDS
  # 19. Ensembl
  # 20. Ensembl_TRS
  # 21. Ensembl_PRO
  # 22. Additional PubMed
  if not args['--quiet']:
    print "\nProcessing {} rows in file {}".format(line_ct, fn)
  with open(fn, 'rU') as tsv:
    ct = 0
    tmark = set()
    xref_ct = 0
    skip_ct = 0
    dba_err_ct = 0
    for line in tsv:
      ct += 1
      slmf.update_progress(ct/line_ct)
      data = line.split('\t')
      up = data[0]
      if not data[4]: # no gi(s)
        skip_ct += 1
        continue
      tids = dba.find_target_ids({'uniprot': up})
      if not targets:
        skip_ct += 1
        continue
      tid = tids[0]
      for gi in data[4].split('; '):
        rv = dba.ins_xref({'target_id': tid, 'xtype': 'NCBI GI', 'value': gi})
        if rv:
          xref_ct += 1
        else:
          dba_err_ct += 1
      tmark.add(tid)
  print "\n{} rows processed".format(ct)
  print "  Inserted {} new GI xref rows for {} targets".format(xref_ct, len(tmark))
  print "  Skipped {} rows with no GI".format(skip_ct)
  if dba_err_ct > 0:
    print "WARNING: {} database errors occured. See logfile {} for details.".format(dba_err_ct, logfile)


if __name__ == '__main__':
  print "\n{} (v{}) [{}]:".format(PROGRAM, __version__, time.strftime("%c"))
  start_time = time.time()

  args = docopt(__doc__, version=__version__)
  if args['--debug']:
    print "\n[*DEBUG*] ARGS:\n%s\n"%repr(args)
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
    print("Connected to TDLBase: {} (schema ver {}; data ver {})".format(args['--dbname'], dbi['schema_ver'], dbi['data_ver']))

  download(args)
  load(args, dba, logger, logfile)
  
  elapsed = time.time() - start_time
  print "\n{}: Done. Elapsed time: {}\n".format(PROGRAM, slmf.secs2str(elapsed))

