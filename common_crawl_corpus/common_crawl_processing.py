import os
import requests
import gzip
import pandas as pd
from multiprocessing.pool import ThreadPool

import logging

FILE_DOWNLOAD_DIR = "./common_crawl_download"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.debug('call to common_crawl_corpus made')


def download_index(year_range: str) -> None:
    """
        Downloads the top index file for the given year range.
        e.g. CC-MAIN-2022-40
    """
    url = f"https://data.commoncrawl.org/crawl-data/{year_range}/wet.paths.gz"
    logger.info('inside download_index')

    os.makedirs(os.path.abspath(FILE_DOWNLOAD_DIR), exist_ok=True)
    filepath = os.path.abspath(os.path.join(FILE_DOWNLOAD_DIR, f"{year_range}-wet.paths.gz".strip()))
    logger.debug('filepath of downloaded file: %s', str(filepath))
    response = requests.get(url)
    with open(filepath, "wb") as file:
        file.write(response.content)


def download_wet_from_index(index_filename: str):
    """
    Scan downloaded index file and download its segments
    """
    with gzip.open(index_filename, "r") as index:
        lines = [line.decode("utf-8").rstrip() for line in index.readlines()]
        ThreadPool(8).imap_unordered(download_wet, lines)


def download_wet(index: str):
    """
        Downloads the second level index file for the given year range.
        e.g. crawl-data/CC-MAIN-2022-40/segments/1664030331677.90/wet/CC-MAIN-20220924151538-20220924181538-00000.warc.wet.gz
    """
    url = f"https://data.commoncrawl.org/{index}".strip()
    logger.info('inside download_wet')

    os.makedirs(os.path.abspath(FILE_DOWNLOAD_DIR), exist_ok=True)
    filepath = os.path.abspath(os.path.join(FILE_DOWNLOAD_DIR, index.replace("/", "-")).strip())
    logger.debug('filepath of download file: %s', str(filepath))
    response = requests.get(url)
    with open(filepath, "wb") as file:
        file.write(response.content)


def save_df(df: pd.DataFrame, filename: str):
    df.infer_objects()
    # Save to feather, existing df used picked with gzip
    df.to_feather(filename)
    print(f"Saved {filename}")


if __name__ == '__main__':
    pass
