import requests
import os
import warcio

FILE_DOWNLOAD_DIR = "/home/nhan/Downloads/"


def download_from_period(index_level: int, segment: str) -> None:
    """
    Download a first index path from the given period in common crawl.
    :arg
    :param index_level: 1 -
    :param segment: For example, CC-MAIN-2014-15
    """
    if index_level == 1:
        url = f"https://commoncrawl.org/crawl-data/{segment}/warc.paths.gz"
    elif index_level == 2:
        url = f"https://commoncrawl.org/{segment}"
    else:
        raise ValueError(f"Invalid index level: {index_level}, index level should be between 1 and 2")
    with open(os.path.join(FILE_DOWNLOAD_DIR, f"{segment}-warc.paths.gz"), "wb") as file:
        response = requests.get(url)
        file.write(response.content)


def read_warc(filename: str) -> None:
    i = 0
    with open(os.path.join(FILE_DOWNLOAD_DIR, filename),
              "rb") as file:
        for record in warcio.ArchiveIterator(file):
            if record.rec_type == 'response':
                print(record.rec_headers.get_header('WARC-Target-URI'))


if __name__ == '__main__':
    read_warc("CC-MAIN-20140416005201-00000-ip-10-147-4-33.ec2.internal.warc")
