import os
import json
import requests
import urllib
import io
import warcio

import pandas as pd

from tqdm import tqdm



def load_crawls_to_search():
    save_path = os.path.join('processed_data','cc_completed_searches.csv')
    if os.path.exists(save_path):
        completed_searches = pd.read_csv(save_path, header=None, names=['url', 'crawl'])
    else:
        completed_searches = pd.DataFrame(columns=['url', 'crawl'])
    return completed_searches


def read_url_data():
    df = pd.read_excel(os.path.join('input_data', 'hybridhiring', 'DATA_RELEASE.XLSX'),index_col=0)
    return df


def save_htmls(htmls, url, crawl):
    new_row = pd.DataFrame({'url': url, 'crawl': crawl, 'html':htmls})
    save_path = os.path.join('processed_data','cc_htmls.csv')
    if not os.path.exists(save_path):
        new_row.to_csv(save_path, index=False)
    else:
        with open(save_path, 'a') as f:
            new_row.to_csv(f, header=False, index=False)

def log_completed_crawl(url, crawl, was_successful):
    if not was_successful:
        with open(os.path.join('processed_data','cc_completed_searches.csv'), 'a') as f:
            f.write(f'{url},{crawl}\n')
        return 1
    else:
        all_crawls = pd.read_csv(os.path.join('input_data', 'crawl_list.csv'), header=None)
        all_crawls.columns = ['crawl']
        crawl_iloc = all_crawls[all_crawls['crawl'] == crawl].index[0]
        all_crawls = all_crawls.iloc[crawl_iloc:]
        for crawl in all_crawls['crawl']:
            with open(os.path.join('processed_data','cc_completed_searches.csv'), 'a') as f:
                f.write(f'{url},{crawl}\n')
        return len(all_crawls)

def scrape_crawl(crawl, url):
    encoded_url = urllib.parse.quote(url, safe='')
    resp = requests.get(
        f'http://index.commoncrawl.org/{crawl}-index?url={encoded_url}&output=json')
    if resp.status_code != 200:
        return log_completed_crawl(url, crawl, was_successful=False)
    pages = [json.loads(x) for x in resp.content.decode('utf-8').strip().split('\n')]

    htmls = []
    for page in pages:
        offset, length = int(page['offset']), int(page['length'])
        offset_end = offset + length - 1

        prefix = 'https://data.commoncrawl.org/'

        resp = requests.get(prefix + page['filename'], headers={'Range': 'bytes={}-{}'.format(offset, offset_end)})
        try:
            with io.BytesIO(resp.content) as stream:
                for record in warcio.ArchiveIterator(stream):
                    htmls.append(record.content_stream().read())
        except warcio.exceptions.ArchiveLoadFailed:
            return log_completed_crawl(url, crawl, was_successful=False)

    save_htmls(htmls, url, crawl)
    return log_completed_crawl(url, crawl, was_successful=True)





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

    with tqdm(total=len(to_search), smoothing=0) as pbar:
        completed_url = None
        for _, (url, crawl) in to_search[['bio_url','crawl']].iterrows():
            if not url == completed_url:
                completed = scrape_crawl(crawl, url)
                pbar.update(completed)
            if completed > 1:
                completed_url = url
