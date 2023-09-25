import os
import gzip
import json
import requests
import urllib
import io
import warcio

import pandas as pd

from tqdm import tqdm

from bs4 import BeautifulSoup


def load_crawls_to_search():
    save_path = os.path.join('processed_data','completed_searches.csv')
    if os.path.exists(save_path):
        completed_searches = pd.read_csv(save_path, header=None)
    else:
        completed_searches = pd.DataFrame(columns=['url', 'crawl'])
    return completed_searches


def read_url_data():
    df = pd.read_excel(os.path.join('input_data', 'hybridhiring', 'DATA_RELEASE.XLSX'),index_col=0)
    return df


def save_htmls(htmls, url, crawl):
    if not os.path.exists('htmls'):
        os.mkdir('htmls')
    if not os.path.exists(os.path.join('htmls', crawl)):
        os.mkdir(os.path.join('htmls', crawl))
    if not os.path.exists(os.path.join('htmls', crawl, url)):
        os.mkdir(os.path.join('htmls', crawl, url))
    for i, html in enumerate(htmls):
        with open(os.path.join('htmls', crawl, url, f'{i}.html'), 'wb') as f:
            f.write(html)

def log_completed_crawl(url, crawl):
    with open(os.path.join('processed_data','completed_searches.csv'), 'a') as f:
        f.write(f'{url},{crawl}\n')

def scape_crawl(crawl, url):
    encoded_url = urllib.parse.quote(url, safe='')
    resp = requests.get(
        f'http://index.commoncrawl.org/{crawl}-index?url={encoded_url}&output=json')
    if resp.status_code != 200:
        log_completed_crawl(url, crawl)
        return None
    pages = [json.loads(x) for x in resp.content.decode('utf-8').strip().split('\n')]

    htmls = []
    for page in pages:
        offset, length = int(page['offset']), int(page['length'])
        offset_end = offset + length - 1

        prefix = 'https://data.commoncrawl.org/'

        resp = requests.get(prefix + page['filename'], headers={'Range': 'bytes={}-{}'.format(offset, offset_end)})
        with io.BytesIO(resp.content) as stream:
            for record in warcio.ArchiveIterator(stream):
                htmls.append(record.content_stream().read())
    save_htmls(htmls, url, crawl)
    log_completed_crawl(url, crawl)





if __name__ == '__main__':
    # Find urls that need to be scraped
    urls = read_url_data()

    # Find list of common crawls that need to be searched
    crawl_list = pd.read_csv(os.path.join('input_data', 'crawl_list.csv'), header=None)

    # Merge the two lists to find which urls need to be searched in which crawls
    crawl_list.columns = ['crawl']
    crawl_list['join_key'] = 1
    urls['join_key'] = 1
    urls = urls.merge(crawl_list, on='join_key')


    # Find list of url/crawls combos already searched
    already_completed = load_crawls_to_search()
    to_search = urls.merge(already_completed, how='left', left_on=['bio_url','crawl'], right_on=['url', 'crawl'], indicator=True)
    to_search = to_search[to_search['_merge'] == 'left_only']
    to_search = to_search.drop(columns=['_merge', 'join_key'])

    with tqdm(total=len(to_search)) as pbar:
        for _, (url, crawl) in to_search[['bio_url','crawl']].iterrows():
            scape_crawl(crawl, url)
            pbar.update(1)
