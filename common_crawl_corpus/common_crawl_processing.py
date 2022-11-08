import os
import pickle

import pandas as pd
import requests

FILE_DOWNLOAD_DIR = "/Volumes/nathan/Common Crawl/"


# FILE_DOWNLOAD_DIR = "/Users/nathan/Downloads"


def download_index(year_range: str) -> None:
    """
        Downloads the top index file for the given year range.
        e.g. CC-MAIN-2014-15
    """
    url = f"https://data.commoncrawl.org/crawl-data/{year_range}/wet.paths.gz"
    with open(os.path.join(FILE_DOWNLOAD_DIR, f"{year_range}-warc.paths.gz".replace("/", "-")), "wb") as file:
        response = requests.get(url)
        file.write(response.content)


def download_wet(index: str):
    """
        Downloads the second level index file for the given year range.
        e.g. crawl-data/CC-MAIN-2022-40/segments/1664030331677.90/wet/CC-MAIN-20220924151538-20220924181538-00000.warc.wet.gz
    """
    url = f"https://data.commoncrawl.org/{index}"
    with open(os.path.join(FILE_DOWNLOAD_DIR, index.replace("/", "-")), "wb") as file:
        response = requests.get(url)
        file.write(response.content)


def ngrams_deduplicate():
    pass


def save_df(df: pd.DataFrame, filename: str):
    df.infer_objects()
    # HIGHEST_PROTOCOL for python3.8+, DEFAULT_PROTOCOL for 3.4+
    df.to_pickle(filename, compression="gzip", protocol=pickle.HIGHEST_PROTOCOL)
    print(f"Saved {filename}")


if __name__ == '__main__':
    pass
    # download_wet("crawl-data/CC-MAIN-2014-15/segments/1397609521512.15/wet/CC-MAIN-20140416005201-00000-ip-10-147-4-33.ec2.internal.warc.wet.gz")
