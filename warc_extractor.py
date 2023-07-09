import os
import time
import json
import logging
import datetime
import argparse
import concurrent.futures
from typing import List
from warcio import ArchiveIterator
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

DATASET_NEWS_CC = "news-cc"
DEFAULT_LIMIT_FOR_TESTING = 10
DEFAULT_COMMON_CRAWL_DATA_DIR = "./commoncrawl-data"
COMMON_CRAWL_DATA_DIR = os.environ.get("COMMON_CRAWL_DATA_DIR", DEFAULT_COMMON_CRAWL_DATA_DIR)

DEFAULT_WARC_EXTRACT_DIR = "warc-extract"
WARC_EXTRACT_DIR = os.environ.get("WARC_EXTRACT_DIR", DEFAULT_WARC_EXTRACT_DIR)

DEFAULT_PROCESSED_CONTENT_DIR = "processed-content"
PROCESSED_CONTENT_DIR = os.environ.get("PROCESSED_CONTENT_DIR", DEFAULT_PROCESSED_CONTENT_DIR)

JSON_OUT_FILE_EXT = ".json"
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


def list_files(dir_name: str, limit: bool = False, limit_value: int = DEFAULT_LIMIT_FOR_TESTING):
    files = list()
    for (dirpath, dirnames, filenames) in os.walk(dir_name):
        files += [os.path.join(dirpath, file) for file in filenames]
    print(f"{len(files)} to process...")

    if limit:
        files = files[:limit_value]
        print(f"limit is True... processing {limit_value} files... probably in TEST mode!!")

    return files


def list_files_filter_suffix(directory_name, file_suffix):
    list_of_files = list()
    for root, dirs, files in os.walk(directory_name):
        for file in files:
            if file.endswith(file_suffix):
                list_of_files.append(os.path.join(root, file))
    return list_of_files


def get_warc_file_name(warc_file_path):
    return warc_file_path.split(".warc")[0]

def is_camel_case(word):
    count = 0
    for i in range(0, len(word) - 1):
        if (word[i].isupper()):
            count += 1
    return count > 1

def num_camel_case_words(text):
    counter = 0
    for word in text.split(" "):
        if is_camel_case(word):
            counter = counter + 1

    return counter

def write_json_to_file(dirs: List, file_name, data):
    dir_path = os.path.join(*dirs)
    os.makedirs(dir_path, exist_ok=True)
    path = os.path.join(dir_path, file_name)
    print(f"saving data to {path}...")
    with(open(path, "w+")) as out_file:
        out_file.writelines(json.dumps(data))


class CommonCrawlWarcExtractor:
    def process(self, warc_file_path, limit=False):
        warc_file_name = get_warc_file_name(warc_file_path)
        try:
            os.makedirs(warc_file_name, exist_ok=True)
            logging.debug(f"created {warc_file_name} directory!")
        except OSError as error:
            logging.error(f"could not create {warc_file_name} directory!")
            exit(1)

        num_processed = 0
        with open(warc_file_path, 'rb') as stream:
            futures = []
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for record in ArchiveIterator(stream):
                    if record.rec_type == 'request':
                        continue

                    if record.rec_type == 'response':
                        warc_record_id = record.rec_headers.get_header('WARC-Record-ID')
                        warc_target_uri = record.rec_headers.get_header('WARC-Target-URI')
                        warc_date = record.rec_headers.get_header('WARC-Date')
                        warc_content_length = record.rec_headers.get_header('Content-Length')

                        if not warc_target_uri or int(warc_content_length) <= 0:
                            logging.warning(
                                f'WARNING: did not extract content for: {warc_target_uri}... no warc_target_uri, or warc_content_length is 0')
                            return

                        domain = urlparse(warc_target_uri).netloc

                        logging.info(f'extracting content for: {warc_target_uri}...')

                        article_html = record.content_stream().read()

                        record_processor_wrapper = RecordProcessorWrapper(warc_file_name, warc_record_id,
                                                                          warc_target_uri,
                                                                          warc_content_length, warc_date,
                                                                          domain, article_html)
                        num_processed = num_processed + 1
                        futures.append(executor.submit(record_processor_wrapper.process))

            for future in concurrent.futures.as_completed(futures):
                future.result()

        return num_processed

class RecordProcessorWrapper():
    def __init__(self, warc_file_name, warc_record_id,
                 warc_target_uri, warc_content_length,
                 warc_date, domain, article_html):
        self.warc_file_name = warc_file_name
        self.warc_record_id = warc_record_id
        self.warc_target_uri = warc_target_uri
        self.warc_content_length = warc_content_length
        self.warc_date = warc_date
        self.domain = domain
        self.article_html = article_html

    def process(self):
        try:
            dataset_id = self.warc_record_id.split(":")[-1].split(">")[0]
            data = {
                "dataset_id": dataset_id,
                "dataset": DATASET_NEWS_CC,
                "dataset_content_length": self.warc_content_length,
                "uri": self.warc_target_uri,
                "warc_sourced_date": self.warc_date,
                "warc_extracted_date": datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
                "domain": self.domain,
                "article_html": self.article_html.decode("utf-8", "ignore")
            }

            file_name = dataset_id + JSON_OUT_FILE_EXT
            write_json_to_file([self.warc_file_name, WARC_EXTRACT_DIR, self.domain], file_name, data=data)

        except Exception as e:
            logging.error('An exception occurred: {}'.format(e))

if __name__ == '__main__':
    start_time = time.time()
    parser = argparse.ArgumentParser()
    parser.add_argument('--warc-file-path', type=str, required=True, help='full path to compressed warc file')
    args = parser.parse_args()

    if not args.warc_file_path:
        logging.error("warc-file-path is required!")
        print(parser.print_help())
        exit(1)

    file_path = os.path.join(args.warc_file_path)
    logging.info(f"processing warc file: {file_path}...")
    common_crawl_warc_extractor = CommonCrawlWarcExtractor()
    num_processed = common_crawl_warc_extractor.process(file_path, limit=False)
    metrics = {
        "took": (time.time() - start_time),
        "num_processed": num_processed
    }
    logging.info(metrics)
    exit(0)