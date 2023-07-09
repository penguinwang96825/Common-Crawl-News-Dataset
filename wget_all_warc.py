import os
from tqdm.auto import tqdm


def main():
    with open('warc.paths', 'r') as f:
        lines = [line.strip() for line in f]
    urls = [f'https://data.commoncrawl.org/{line}' for line in lines]

    for url in tqdm(urls):
        os.system(f'wget {url}')


if __name__ == '__main__':
    main()