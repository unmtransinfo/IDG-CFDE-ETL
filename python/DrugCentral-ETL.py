#!/usr/bin/env python
"""
DrugCentral ETL Script. Extracts all DrugCentral activities against human targets and writes the data to a CSV file.

Usage: 
    DrugCentral-ETL.py [--debug | --quiet] [--host=<str>] [--port=<int>] [--dbname=<str>] [--user=<str>] [--password=<str>] [--outfile=<str>]
    DrugCentral-ETL.py [--help | --version]

Options:
  -h --help          print this message and exit
  -v --version       print version and exit
  --host=DBHOST      The hostname or IP address of the PostgreSQL server [default: unmtid-dbs.net]
  --port=DBPORT      The port number of the PostgreSQL server [default: 5433]
  --dbname=DBNAME    The name of the database [default: drugcentral]
  --user=DBUSER      The username for authentication [default: drugman]
  --password=DBPASS  The password for authentication [default: dosage]
  --outfile=OFN      Filename for the output CSV file [default: DrugCentralActivities.csv]
  --quiet            set output verbosity to minimal level
  --debug            write debugging output to logfile ../log/DrugCentral-ETL.log

"""
__author__    = "Steve Mathias"
__email__     = "smathias @salud.unm.edu"
__org__       = "Translational Informatics Division, UNM School of Medicine"
__copyright__ = "Copyright 2025, Steve Mathias"
__license__   = "Creative Commons Attribution-NonCommercial (CC BY-NC)"
__version__   = "0.1.1"

import os,sys,time
import platform
from docopt import docopt
import bonobo
from bonobo.config import use_context_processor
import psycopg2
from psycopg2.extras import RealDictCursor
from icecream import ic
import csv

PROGRAM = os.path.basename(sys.argv[0])
LOGFILE = f"../log/{PROGRAM}.log"
DC_ACTS_SQL = """SELECT atf.*, s.name, s.smiles 
                 FROM act_table_full atf, structures s 
                 WHERE atf.struct_id = s.cd_id AND 
                       atf.organism = 'Homo sapiens'"""
# The header fields define the data that is output to the CSV file
CSV_HEADER = ['target_name', 'target_class', 'gene', 'uniprot', 'swissprot', 'ligand_name', 'ligand_smiles', 
              'act_type', 'relation', 'act_value', 'action_type', 'act_source', 'act_source_url', 'act_comment', 
              'moa', 'moa_source', 'moa_source_url', 'first_in_class']
HEADER_FLAG = False

def construct_pg_dsn(
    hostname: str = "localhost",
    port: int = 5432,
    dbname: str = "postgres",
    user: str = "postgres",
    password: str = "",
    options: str = None,
    sslmode: str = None
) -> str:
  """
  Constructs a valid DSN (Data Source Name) for connecting to a PostgreSQL database.
  
  Arguments:
    hostname (str): The hostname or IP address of the PostgreSQL server. Default is "localhost".
    port (int): The port number of the PostgreSQL server. Default is 5432.
    dbname (str): The name of the database. Default is "postgres".
    user (str): The username for authentication. Default is "postgres".
    password (str): The password for authentication. Default is an empty string.
    options (str, optional): Additional connection options (e.g., "-c search_path=schema"). Default is None.
    sslmode (str, optional): SSL mode for the connection (e.g., "require", "disable"). Default is None.
    
  Returns:
    str: A valid PostgreSQL DSN string.
  """
  # Start with the basic DSN components
  dsn_parts = [ f"host={hostname}", f"port={port}", f"dbname={dbname}", f"user={user}", f"password={password}" ]
  # Add optional components if provided
  if options:
    dsn_parts.append(f"options={options}")
  if sslmode:
    dsn_parts.append(f"sslmode={sslmode}")
  # Join all parts into the DSN string
  dsn = " ".join(dsn_parts)
  
  return dsn
  
def extract(dsn: str) -> None:
  conn = psycopg2.connect(dsn)
  ic(conn)
  with conn.cursor(cursor_factory=RealDictCursor) as cursor:
    cursor.execute(DC_ACTS_SQL)
    for d in cursor:
      yield d
  conn.close()
  
def transform(actd):
  actd["uniprot"] = actd.pop("accession")
  actd["ligand_name"] = actd.pop("name")
  actd["ligand_smiles"] = actd.pop("smiles")
  for k in ['act_id', 'struct_id', 'target_id', 'act_ref_id', 'moa_ref_id', 'organism', 'tdl']:
    del actd[k]
  #ic(actd)  
  csvlst = []
  for k in CSV_HEADER:
    if actd[k] != None:
      csvlst.append( str(actd[k]) )
    else:
      csvlst.append('')
  csvstr = ",".join(csvlst)
  
  return csvstr
  
def load(csvstr):
  global HEADER_FLAG
  if HEADER_FLAG == False:
    header = ",".join(CSV_HEADER)
    print(header)
    print(csvstr)
    HEADER_FLAG = True
  else:
    print(csvstr)

def with_opened_file(self, context):
  global OUT_FN  
  with context.get_service('fs').open(OUT_FN, 'w+') as fh:
    yield fh
    
@use_context_processor(with_opened_file)
def write_repr_to_file(fh, *row):
  fh.write(repr(row) + "\n")  
  
@use_context_processor(with_opened_file)
def write_csv_to_file(fh, csvstr):
  global HEADER_FLAG
  if HEADER_FLAG == False:
    header = ",".join(CSV_HEADER)
    fh.write(f"{header}\n")
    fh.write(f"{csvstr}\n")
    HEADER_FLAG = True
  else:
    fh.write(f"{csvstr}\n")

def log_to_file(msg: str):
  with open(LOGFILE, "a") as lfh:
    lfh.write(msg + "\n")


if __name__ == "__main__":
  args = docopt(__doc__, version=__version__)
  if args['--quiet']:
    ic.disable()
  elif args['--debug']:
    ic.configureOutput(prefix="DEBUG| ", outputFunction=log_to_file)
    ic(args)
  OUT_FN = args['--outfile']

  print("\n{} (v{}) [{}]:\n".format(PROGRAM, __version__, time.strftime("%c")))

  dc_dsn = construct_pg_dsn( hostname = args['--host'],
                             port = args['--port'],
                             dbname = args['--dbname'],
                             user =  args['--user'],
                             password = args['--password'] )
  ic(dc_dsn)
  
  # Create the ETL pipeline graph
  graph = bonobo.Graph( extract(dc_dsn),
                        transform,
                        #load,
                        write_csv_to_file 
                       )
  ic(graph)
  # Run the pipeline
  bonobo.run(graph)

  
    


