# PubMed Data Processor

This is a simple script to process the PMC PubMed Open Access Dataset. The data can be obtained from the PMC FTP
server as described [here](https://www.ncbi.nlm.nih.gov/pmc/tools/openftlist/). Only the documents for commercial
use should be downloaded:

1. Click on the "PMC Open Access Subset - Bulk" link
2. Click "oa_comm" and then "xml"
3. Download the filelist.csv and tar.gz files from each baseline
4. Place the files in the same directory as the script under a directory called "assets". Each baseline should be
   placed in a subdirectory corresponding to the PMC prefix i.e. `assets/000`, `assets/001`, etc. There should be
   10 in total (000-009)

## Installation

1. Install [Python 3.9](https://www.python.org/downloads/) or greater
2. Install [Poetry](https://python-poetry.org/docs/#installation)
3. Run `poetry install` to install the dependencies

## Usage

Run the bash script `run.sh` to process the data. Each XML file will be parsed and saved as a Parquet file under
`processed.parquet` in the relative subdirectory of assets i.e. `assets/000/processed.parquet`, `assets/001/processed.parquet`, etc.

To manage the size of datasets, data is processed in batches and saved as parts. You can control the number of parts
(and subsequently parquet files per baseline) by including the `--parquet_chunks` argument in the call to
`pubmed_data/process.py` in `run.sh`.

Other arguments include:

- --filelist: the path to the filelist.csv file for the baseline
- --tar: the path to the tar.gz file for the baseline
- --n_jobs: the number of processes to use for processing the data (default: -1)
- --log: the path to the log file (default: `logs/pubmed.log`)
- --log_level: the log level (default: `INFO`)

Each document XML file is parsed and the following fields are extracted and populated in the resulting Parquet files:

- valid (int): 1 if the document is valid and was correctly parsed by Pubmed Parser, 0 otherwise and should be ignored
  in subsequent analysis
- title (str): the full title of the document
- abstract (str): the full abstract of the document
- citations (str): PMIDs of the citations in the document stored as a comma-separated string
- pmid (str): the PMID of the document
- pmc (str): the PMC ID of the document
- doi (str): the DOI of the document
- journal (str): the journal name of the document
- processing_date (str): the date the document was parsed by Pubmed Parser
