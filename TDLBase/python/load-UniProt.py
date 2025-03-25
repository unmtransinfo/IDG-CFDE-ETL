#!/usr/bin/env python3
# Time-stamp: <2025-02-13 14:47:16 smathias>
"""Load human reviewed protein data from UniProt.org into a TDLBase MySQL DB.

Usage:
    load-UniProt.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-UniProt.py -? | --help

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
  -d --debug           : turn on debugging output to console
  -? --help            : print this message and exit 
"""
__author__    = "Steve Mathias"
__email__     = "smathias @salud.unm.edu"
__org__       = "Translational Informatics Division, UNM School of Medicine"
__copyright__ = "Copyright 2025, Steve Mathias"
__license__   = "Creative Commons Attribution-NonCommercial (CC BY-NC)"
__version__   = "1.0.0"

import os,sys,time,re
from docopt import docopt
from TDLB.Adaptor import Adaptor
import logging
from urllib.request import urlretrieve
import gzip
import obo
from lxml import etree, objectify
import slm_util_functions as slmf
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

PROGRAM = os.path.basename(sys.argv[0])
LOGDIR = f"../log/TDLBase/"
LOGFILE = f"{LOGDIR}/{PROGRAM}.log"
UP_DOWNLOAD_DIR = '../data/UniProt/'
UP_BASE_URL = 'ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/'
UP_HUMAN_FILE = 'uniprot_sprot_human.xml.gz'
NS = '{https://uniprot.org/uniprot}'
ECO_BASE_URL = 'https://raw.githubusercontent.com/evidenceontology/evidenceontology/master/'
ECO_DOWNLOAD_DIR = '../data/EvidenceOntology/'
ECO_OBO = 'eco.obo'

def download_eco(args):
  if os.path.exists(ECO_DOWNLOAD_DIR + ECO_OBO):
    os.remove(ECO_DOWNLOAD_DIR + ECO_OBO)
  if not args['--quiet']:
      print("\nDownloading {}".format(ECO_BASE_URL + ECO_OBO))
      print("         to {}".format(ECO_DOWNLOAD_DIR + ECO_OBO))
  urlretrieve(ECO_BASE_URL + ECO_OBO, ECO_DOWNLOAD_DIR + ECO_OBO)

def mk_eco_map(args):
  """
  Return a mapping of Evidence Ontology ECO IDs to Go Evidence Codes.
  """
  fn = ECO_DOWNLOAD_DIR + ECO_OBO
  if not args['--quiet']:
    print(f"\nParsing Evidence Ontology file {fn}")
  eco = {}
  eco_map = {}
  parser = obo.Parser(fn)
  for stanza in parser:
    eco[stanza.tags['id'][0].value] = stanza.tags
  regex = re.compile(r'GOECO:([A-Z]{2,3})')
  for e,d in eco.items():
    if not e.startswith('ECO:'):
      continue
    if 'xref' in d:
      for x in d['xref']:
        m = regex.match(x.value)
        if m:
          eco_map[e] = m.group(1)
  return eco_map

def download_uniprot(args):
    gzfn = UP_HUMAN_FILE
    gzfp = UP_DOWNLOAD_DIR + gzfn
    fp = gzfp.replace('.gz', '')
    if os.path.exists(gzfp):
      os.remove(gzfp)
    if os.path.exists(fp):
      os.remove(fp)
    if not args['--quiet']:
      print("\nDownloading {}".format(UP_BASE_URL + gzfn))
      print(f"         to {gzfn}")
    urlretrieve(UP_BASE_URL + gzfn, gzfp)
    if not args['--quiet']:
      print(f"Uncompressing {gzfp}")
    ifh = gzip.open(gzfp, 'rb')
    ofh = open(fp, 'wb')
    ofh.write( ifh.read() )
    ifh.close()
    ofh.close()
  
def load_targets(args, dba, eco_map, logger, logfile):
  fn = UP_DOWNLOAD_DIR + UP_HUMAN_FILE.replace('.gz', '')
  if not args['--quiet']:
    print(f"\nParsing file {fn}")
  root = objectify.parse(fn).getroot()
  up_ct = len(root.entry)
  if not args['--quiet']:
    print(f"Loading data for {up_ct} UniProt records")
  logger.info(f"Loading data for {up_ct} UniProt records in file {fn}")
  ct = 0
  load_ct = 0
  xml_err_ct = 0
  dba_err_ct = 0
  for i in range(len(root.entry)):
    ct += 1
    slmf.update_progress(ct/up_ct)
    entry = root.entry[i]
    logger.info("Processing entry {}".format(entry.accession))
    tinit = entry2tinit(entry, eco_map)
    if not tinit:
      xml_err_ct += 1
      logger.error("XML Error for {}".format(entry.accession))
      continue
    tid = dba.ins_target(tinit)
    if not tid:
      dba_err_ct += 1
      continue
    logger.debug(f"Target insert id: {tid}")
    load_ct += 1
  print(f"Processed {ct} UniProt records.")
  print(f"  Loaded {load_ct} targets")
  if xml_err_ct > 0:
    print(f"WARNING: {xml_err_ct} XML parsing errors occurred. See logfile {logfile} for details.")
  if dba_err_ct > 0:
    print(f"WARNING: {dba_err_ct} DB errors occurred. See logfile {logfile} for details.")

def get_entry_by_accession(root, acc):
  """
  This is for testing/debugging purposes (E.g. IPython)
  """
  for i in range(len(root.entry)):
    entry = root.entry[i]
    if entry.accession == acc:
      return entry
  return None
                                              
def entry2tinit(entry, e2e):
  """
  Convert an entry element of type lxml.objectify.ObjectifiedElement parsed from a UniProt XML entry and return a dictionary suitable for passing to TDLB.Adaptor.ins_target().
  """
  target = {'name': entry.name.text, 'description': entry.protein.recommendedName.fullName.text, 'uniprot': entry.accession.text}
  target['sym'] = None
  aliases = []
  if entry.find(NS+'gene'):
    if entry.gene.find(NS+'name'):
      for gn in entry.gene.name: # returns all gene.names
        if gn.get('type') == 'primary':
          target['sym'] = gn.text
        elif gn.get('type') == 'synonym':
          # HGNC symbol alias
          aliases.append( {'atype': 'symbol', 'value': gn.text} )
  target['seq'] = str(entry.sequence).replace('\n', '')
  target['up_version'] = entry.sequence.get('version')
  for acc in entry.accession: # returns all accessions
    if str(acc) != target['uniprot']:
      aliases.append( {'atype': 'uniprot', 'value': str(acc)} )
  if entry.protein.recommendedName.find(NS+'shortName') != None:
    sn = entry.protein.recommendedName.shortName.text
    aliases.append( {'atype': 'uniprot', 'value': sn} )
  target['aliases'] = aliases
  # Function and Family TDL Infos (from comments)
  tdl_infos = []
  if entry.find(NS+'comment'):
    for c in entry.comment:
      if c.get('type') == 'function':
        tdl_infos.append( {'itype': 'UniProt Function',  'string_value': str(c.getchildren()[0])} )
      if c.get('type') == 'similarity':
        tdl_infos.append( {'itype': 'UniProt Family',  'string_value': str(c.getchildren()[0])} )
  target['tdl_infos'] = tdl_infos
  # GeneID, XRefs, GOAs from dbReferences
  xrefs = []
  goas = []
  for dbr in entry.dbReference:
    if dbr.attrib['type'] == 'GeneID':
      # Some UniProt records have multiple Gene IDs
      # So, only take the first one and fix manually after loading
      if 'geneid' not in target:
        target['geneid'] = str(dbr.attrib['id'])
    elif dbr.attrib['type'] in ['InterPro', 'Pfam', 'PROSITE', 'SMART']:
      xtra = None
      for el in dbr.findall(NS+'property'):
        if el.attrib['type'] == 'entry name':
          xtra = str(el.attrib['value'])
        xrefs.append( {'xtype': str(dbr.attrib['type']),
                       'value': str(dbr.attrib['id']), 'xtra': xtra} )
    elif dbr.attrib['type'] == 'GO':
      name = None
      goeco = None
      assigned_by = None
      for el in dbr.findall(NS+'property'):
        if el.attrib['type'] == 'term':
          name = str(el.attrib['value'])
        elif el.attrib['type'] == 'evidence':
          goeco = str(el.attrib['value'])
        elif el.attrib['type'] == 'project':
          assigned_by = str(el.attrib['value'])
        if goeco in e2e:
          goas.append( {'go_id': str(dbr.attrib['id']), 'go_term': name,
                        'goeco': goeco, 'evidence': e2e[goeco],
                        'assigned_by': assigned_by} )
    elif dbr.attrib['type'] == 'Ensembl':
      xrefs.append( {'xtype': 'Ensembl', 'value': str(dbr.attrib['id'])} )
      for el in dbr.findall(NS+'property'):
        if el.attrib['type'] == 'protein sequence ID':
          xrefs.append( {'xtype': 'Ensembl', 'value': str(el.attrib['value'])} )
        elif el.attrib['type'] == 'gene ID':
          xrefs.append( {'xtype': 'Ensembl', 'value': str(el.attrib['value'])} )
    elif dbr.attrib['type'] == 'STRING':
      xrefs.append( {'xtype': 'STRING', 'value': str(dbr.attrib['id'])} )
    elif dbr.attrib['type'] == 'DrugBank':
      xtra = None
      for el in dbr.findall(NS+'property'):
        if el.attrib['type'] == 'generic name':
          xtra = str(el.attrib['value'])
      xrefs.append( {'xtype': 'DrugBank', 'value': str(dbr.attrib['id']),
                     'xtra': xtra} )
    elif dbr.attrib['type'] in ['BRENDA', 'ChEMBL', 'MIM', 'PANTHER', 'PDB', 'RefSeq', 'UniGene']:
        xrefs.append( {'xtype': str(dbr.attrib['type']), 'value': str(dbr.attrib['id'])} )
  target['goas'] = goas
  # Keywords
  for kw in entry.keyword:
    xrefs.append( {'xtype': 'UniProt Keyword', 'value': str(kw.attrib['id']),
                   'xtra': str(kw)} )
  target['xrefs'] = xrefs
  return target
  

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
    print("Connected to TDLBase: {} (schema ver {}; data ver {})".format(args['--dbname'], dbi['schema_ver'], dbi['data_ver']))

  download_eco(args)
  download_uniprot(args)

  # UniProt uses ECO IDs in GOAs, not GO evidence codes, so get a mapping of
  # ECO IDs to GO evidence codes
  eco_map = mk_eco_map(args)
  
  load_targets(args, dba, eco_map, logger, logfile)
  
  elapsed = time.time() - start_time
  print("\n{}: Done. Elapsed time: {}\n".format(PROGRAM, slmf.secs2str(elapsed)))
