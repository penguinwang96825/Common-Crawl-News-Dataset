import argparse
import concurrent.futures
import json
import logging
import os
import time
from datetime import datetime
from typing import Optional, List, Any

from pydantic import Field
from pydantic.main import BaseModel
from urllib.parse import urlparse
from newsplease import NewsPlease, NewscrawlerItem


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

class Article(BaseModel):
    url: str
    title: str
    authors: List[str]
    content: str
    excerpt: Optional[str] = None
    content_length: int
    published_date: datetime
    language: str
    domain: str
    media: Optional[str] = None
    meta_info: Optional[Any] = None

    class Config:
        orm_mode = True

    def get_authors_str(self):
        return ",".join(self.authors)

    @classmethod
    def from_orm(cls, obj: Any) -> 'Article':
        # `obj` is the orm model instance
        if hasattr(obj, 'authors'):
            obj.authors = obj.authors.split(',')
        return super().from_orm(obj)

class ArticleSearchParams(BaseModel):
    start_date: Optional[datetime]
    end_date: Optional[datetime]

class ParseArticleInput(BaseModel):
    url: str
    html: str

class NewsPleaseAdapter():
    def __init__(self, article: NewscrawlerItem, url: str):
        self.article = article
        self.url = url
        self.domain = urlparse(url).netloc


    def _process_article(self) -> dict:
        published_date = None
        if self.article.date_publish:
            published_date = self.article.date_publish.strftime('%Y-%m-%dT%H:%M:%SZ')

        return {
            'title': self.article.title,
            'authors': self.article.authors,
            'content': self.article.maintext,
            'excerpt': self.article.description,
            'content_length': len(self.article.maintext),
            'published_date': published_date,
            'url': self.url,
            'domain': self.domain,
            'media': self.article.image_url,
            'language': self.article.language,
        }

    def get_article(self) -> Article:
        return Article(**self._process_article())

class NewsPleaseUrlAdapter(NewsPleaseAdapter):
    def __init__(self, url):
        super().__init__(NewsPlease.from_url(url, timeout=60), url=url)

class NewsPleaseHtmlAdapter(NewsPleaseAdapter):
    def __init__(self, html, url):
        super().__init__(NewsPlease.from_html(html, fetch_images=False), url=url)

def get_warc_file_name(warc_file_path):
    return warc_file_path.split(".warc")[0]


def write_json_to_file(dirs: List, file_name, data):
    dir_path = os.path.join(*dirs)
    os.makedirs(dir_path, exist_ok=True)
    path = os.path.join(dir_path, file_name)
    print(f"saving data to {path}...")
    with(open(path, "w+")) as out_file:
        out_file.writelines(json.dumps(data))

# add list of sites to skip
stop_sites = []

def process_warc_content_dir(root_dir):
    num_processed = 0
    warc_extract_dir = os.path.join(root_dir, WARC_EXTRACT_DIR)

    try:
        logging.debug(f'processing warc content dir: {warc_extract_dir}...')
        futures = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for root, dirs, files in os.walk(warc_extract_dir):
                for file_name in files:
                    full_path = os.path.join(root, file_name)
                    if full_path.endswith(JSON_OUT_FILE_EXT):
                        logging.debug(f'processing file: {full_path}...')
                        num_processed = num_processed + 1
                        content_processor_wrapper = ContentProcessorWrapper(file_name=full_path, root_dir=root_dir)
                        futures.append(executor.submit(content_processor_wrapper.process_warc_content))

        for future in concurrent.futures.as_completed(futures):
            future.result()


    except Exception as e:
        print(f'exception occurred processing warc content dir: {warc_extract_dir} -> {e}')

    return num_processed


class ContentProcessorWrapper():
    def __init__(self, file_name, root_dir):
        self.file_name = file_name
        self.root_dir = root_dir

    def process_warc_content(self):
        try:
            with(open(self.file_name, "r+")) as warc_extract_file:
                warc_extract = json.loads(warc_extract_file.read())
                uri = warc_extract["uri"]
                logging.debug(f'extracting content for: {uri}...')

                domain = warc_extract["domain"]
                dataset_id = warc_extract["dataset_id"]
                article_html = warc_extract["article_html"]

                if domain in stop_sites:
                    logging.warning(f'WARNING: did not extract content for: {uri}... domain in stop list')
                    return

                article = NewsPleaseHtmlAdapter(article_html, uri).get_article()

                meta_info = {
                    "dataset_id": dataset_id,
                    "dataset": warc_extract["dataset"],
                    "dataset_content_length": warc_extract["dataset_content_length"],
                    "warc_sourced_date": warc_extract["warc_sourced_date"],
                    "warc_extracted_date": warc_extract["warc_extracted_date"],
                    "warc_processed_date": datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
                }

                article_json = article.dict()
                if article_json["published_date"]:
                    article_json["published_date"] = article_json["published_date"].strftime('%Y-%m-%dT%H:%M:%SZ')

                article_json["meta_info"] = meta_info

                file_name = dataset_id + JSON_OUT_FILE_EXT
                write_json_to_file([self.root_dir, PROCESSED_CONTENT_DIR, domain], file_name, data=article_json)

        except Exception as e:
            print(f'exception occurred processing warc file: {self.file_name} -> {e}')


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
    root_directory = get_warc_file_name(file_path)
    logging.info(f'processing warc file: {file_path}...')
    logging.info(f'root_directory is: {root_directory}...')
    num_processed = process_warc_content_dir(root_dir=root_directory)

    metrics = {
        "took": (time.time() - start_time),
        "num_processed": num_processed
    }
    print(metrics)
    exit(0)