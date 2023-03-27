import argparse
import logging
import os
import shutil
import tarfile
from dataclasses import asdict, dataclass
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd
import pubmed_parser as pp
from joblib import Parallel, delayed
from tqdm.auto import tqdm

logger = logging.getLogger("PubMed-Data")


@dataclass
class PubMedRow:
    valid: int = 0
    title: str = ""
    abstract: str = ""
    citations: str = ""
    pmid: str = ""
    pmc: str = ""
    doi: str = ""
    journal: str = ""
    processing_date: str = ""

    def asdict(self):
        return asdict(self)


def extract_pubmed_meta(filepath: str) -> dict[str, Union[str, int]]:
    """
    Extracts the metadata from a PubMed XML file and returns a dictionary with the following keys:
    - valid: 1 if the file is valid, 0 otherwise; if any error occurs during parsing, the file is considered invalid.
    - title: the title of the article
    - abstract: the abstract of the article
    - citations: a comma-separated list of PMIDs cited in the article
    - pmid: the PMID of the article
    - pmc: the PMC ID of the article
    - doi: the DOI of the article
    - journal: the journal of the article
    - processing_date: the date the file was processed (today's date)


    Parameters
    ----------
    filepath: str
        The path to the XML file to parse

    Returns
    -------
    dict[str, Union[str, int]]
    """
    try:
        meta = pp.parse_pubmed_xml(filepath)
        citation_data = pp.parse_pubmed_references(filepath)

        if citation_data is None or meta is None:
            return PubMedRow(valid=0).asdict()
        if len(meta["abstract"]) == 0 or len(meta["full_title"]) == 0:
            return PubMedRow(valid=0).asdict()

        citations_pmid = [
            str(int(citation["pmid_cited"]))
            for citation in citation_data
            if citation["pmid_cited"]
        ]
        return PubMedRow(
            valid=1,
            title=meta["full_title"],
            abstract=meta["abstract"],
            citations=",".join(citations_pmid),
            pmid=meta["pmid"],
            pmc=meta["pmc"],
            doi=meta["doi"],
            journal=meta["journal"],
            processing_date=datetime.now().strftime("%Y-%m-%d"),
        ).asdict()
    except KeyError:
        logger.exception(
            f"Error while processing {filepath}; one or more keys are missing from the XML file"
        )
        return PubMedRow(valid=0).asdict()
    except TypeError:
        logger.exception(
            f"Error while processing {filepath}; unable to convert PMID to int"
        )
        return PubMedRow(valid=0).asdict()
    except Exception:
        logger.exception(f"Error while processing {filepath}")
        return PubMedRow(valid=0).asdict()


@dataclass
class PubMedProcessor:
    """
    A class to process the PubMed XML files for a single baseline and extract the metadata from them.
    """

    filelist_path: str
    tar_path: str

    def __post_init__(self):
        if not os.path.isfile(self.filelist_path):
            raise FileNotFoundError(f"Filelist {self.filelist_path} not found")
        if not os.path.isfile(self.tar_path):
            raise FileNotFoundError(f"Tarfile {self.tar_path} not found")
        self.root = Path(self.tar_path).parent
        self._extracted_folder_name = os.path.basename(self.tar_path).replace(
            ".tar.gz", ""
        )

    @property
    def extracted_path(self) -> str:
        """
        Returns the path to the extracted folder containing the XML files

        Raises
        ------
        FileNotFoundError
            If the files have not been extracted yet

        Returns
        -------
        str
        """
        if not self.extracted:
            raise FileNotFoundError("Files not extracted")
        # The extracted folder contains a single folder containing all the XML files
        nested_folder = os.listdir(
            os.path.join(self.root, self._extracted_folder_name)
        )[0]
        return os.path.join(self.root, self._extracted_folder_name, nested_folder)

    @property
    def files(self) -> list[str]:
        """
        Returns a list of paths to the XML files in the extracted folder

        Returns
        -------
        list[str]
        """
        return [
            os.path.join(self.extracted_path, file)
            for file in os.listdir(self.extracted_path)
        ]

    @property
    def files_to_exclude(self) -> list[str]:
        """
        Returns a list of paths to the XML files that have been retracted and should be excluded from the dataset.
        The list of retracted files is obtained from the filelist.csv file.

        Returns
        -------
        list[str]
        """
        filelist = pd.read_csv(self.filelist_path)
        exclude = [
            x.split("/")[1]
            for x in filelist[filelist["Retracted"] == "yes"]["Article File"].tolist()
        ]
        return list(
            set(
                [os.path.join(self.extracted_path, file) for file in exclude]
            ).intersection(set(self.files))
        )

    @property
    def extracted(self) -> bool:
        """
        Returns True if the files have been extracted, False otherwise

        Returns
        -------
        bool
        """
        return self._extracted_folder_name in os.listdir(self.root)

    def remove_excluded_files(self):
        """
        Removes the files that have been retracted from the dataset. The list of retracted files is obtained from the
        filelist.csv file.

        Raises
        ------
        FileNotFoundError
            If the files have not been extracted yet

        Returns
        -------
        None
        """
        if not self.extracted:
            raise FileNotFoundError("Files not extracted")
        logger.info(
            f"{len(self.files_to_exclude)} files retracted and will be removed."
        )
        for file in self.files_to_exclude:
            os.remove(file)

    def extract(self):
        """
        Extracts the files from the tar.gz archive

        Returns
        -------
        None
        """
        if self.extracted:
            raise FileExistsError("Already extracted")
        with tarfile.open(self.tar_path) as tar:
            tar.extractall(
                self.root / os.path.basename(self.tar_path).replace(".tar.gz", "")
            )
        self.remove_excluded_files()

    def cleanup(self):
        """
        Removes the extracted folder containing the XML files.

        Raises
        ------
        FileNotFoundError
            If the files have not been extracted yet

        Returns
        -------
        None
        """
        if not self.extracted:
            raise FileNotFoundError("Already cleaned up")
        shutil.rmtree(os.path.join(self.root, self._extracted_folder_name))

    def process(self, n_jobs: int = -1, parquet_chunks: int = 10, **to_parquet_kwargs):
        """
        Processes the XML files and saves them as a batch of parquet files.

        Parameters
        ----------
        n_jobs: int (default=-1)
            Number of jobs to run in parallel. -1 means using all processors.
        parquet_chunks: int (default=10)
            Number of chunks to split the files into. Each chunk will be saved as a separate parquet file.
        to_parquet_kwargs: dict
            Keyword arguments to pass to the pandas.DataFrame.to_parquet method.

        Returns
        -------
        None
        """
        if parquet_chunks < 1:
            raise ValueError("parquet_chunks must be greater than 0")
        if not self.extracted:
            logger.info(f"Extracting files from {self.tar_path}")
            self.extract()
        logger.info(
            f"Processing files from {self.tar_path} and saving as {parquet_chunks} chunks"
        )

        Path(os.path.join(self.root, "processed.parquet")).mkdir(
            parents=True, exist_ok=True
        )
        for i, files_chunk in enumerate(np.array_split(self.files, parquet_chunks)):
            logger.info(f"Chunk {i+1}...")
            with Parallel(n_jobs=n_jobs, backend="multiprocessing") as parallel:
                data = parallel(
                    delayed(extract_pubmed_meta)(file)
                    for file in tqdm(files_chunk, desc="Processing files")
                )
                pd.DataFrame(data).to_parquet(
                    path=os.path.join(
                        self.root, "processed.parquet", f"part_{i}.parquet"
                    ),
                    **to_parquet_kwargs,
                )
        logger.info("Cleaning up extracted files")
        self.cleanup()
        logger.info("Processing complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--filelist", required=True, type=str)
    parser.add_argument("--tar", required=True, type=str)
    parser.add_argument("--n_jobs", default=-1, type=int)
    parser.add_argument("--parquet_chunks", default=10, type=int)
    parser.add_argument("--log", default="logs/pubmed.log", type=str)
    parser.add_argument(
        "--log_level",
        default="INFO",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )

    args = parser.parse_args()
    logger.setLevel(args.log_level)
    log_file_hander = RotatingFileHandler(args.log, maxBytes=1_048_576, backupCount=5)
    log_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    log_file_hander.setFormatter(log_formatter)
    logger.addHandler(log_file_hander)

    processor = PubMedProcessor(args.filelist, args.tar)
    processor.process(n_jobs=args.n_jobs, parquet_chunks=args.parquet_chunks)
