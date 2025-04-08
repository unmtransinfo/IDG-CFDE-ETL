# IDG-CFDE-ETL
This repository contains ETL scripts to process select IDG-KMC
datasets and make them available to CFDE. More details below.

## [DrugCentral](https://www.drugcentral.org)
### Data Source: DrugCentral PostgreSQL database.
### Script: ./python/DrugCentral-ETL.py
This script extracts all acivities against human targets in DrugCentral
and exports the data to a CSV file.


## TDLBase
TDLBase is a minimalist version of
[TCRD](https://habanero.health.unm.edu.tcrd/), containing just the
datasets necessary to generate the Target Development Level (TDL) labels
for all human targets. Like TCRD, TDLBase is a MySQL database, but with
a vastly simplified schema. There is one ETL script per data source, and all
scripts interact with the database via the TDLB.Adaptor API.
./TDLBase/python/TDLB/ :
    - Adaptor.py
    - Create.py
    - Read.py
    - Update.py
    - Delete.py

Currently implemented ETL scripts are:
- load-UniProt.py
- load-HGNC.py
- load-GIs.py

Many ETL scripts yet to be implemented:
- load-ENSGs.py Ensembl Gene IDs
- load-NCBIGene.py
- load-STRINGIDs.py
- load-JensenLabPubMedScores.py
- load-Antibodypedia.py
- load-DrugCentral.py
- load-ChEMBL.py
- load-GuideToPharmacology.py
- load-GOExptFuncLeafTDLIs.py
- load-TDLs.py

Please see ./TDLBase/doc/TDLBase_BuildNotes.org for details about 
the TDLBase build process.

