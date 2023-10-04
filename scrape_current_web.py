import os
import json
import requests
import urllib
import io
import warcio

import pandas as pd

from tqdm import tqdm



def load_crawls_to_search(cc_or_current):
    assert cc_or_current in ['cc', 'current']
    save_path = os.path.join('processed_data',f'{cc_or_current}_completed_searches.csv')
    if cc_or_current == 'cc':
        nms = ['url', 'crawl']
    else:
        nms = ['url']
    if os.path.exists(save_path):
        completed_searches = pd.read_csv(save_path, header=None, names=nms, sep='\t')
    else:
        completed_searches = pd.DataFrame(columns=nms)
    return completed_searches


def read_url_data():
    df = pd.read_excel(os.path.join('input_data', 'hybridhiring', 'DATA_RELEASE.XLSX'),index_col=0)
    return df


def save_htmls(htmls, url, cc_or_current, crawl):
    if crawl is None:
        new_row = pd.DataFrame({'url': url, 'html':htmls})
    else:
        new_row = pd.DataFrame({'url': url, 'crawl': crawl, 'html': htmls})
    save_path = os.path.join('processed_data',f'{cc_or_current}_htmls.csv')
    if not os.path.exists(save_path):
        new_row.to_csv(save_path, index=False)
    else:
        with open(save_path, 'a') as f:
            new_row.to_csv(f, header=False, index=False)

def log_completed_crawl(url, was_successful, cc_or_current, crawl=None):
    assert cc_or_current in ['cc', 'current']
    save_path = os.path.join('processed_data',f'{cc_or_current}_completed_searches.csv')
    if not was_successful or cc_or_current == 'current':
        save_str = f'{url},{crawl}\n' if crawl is not None else f'{url}\n'
        with open(save_path, 'a') as f:
            f.write(save_str)
        return 1
    elif cc_or_current == 'cc':
        all_crawls = pd.read_csv(os.path.join('input_data', 'crawl_list.csv'), header=None)
        all_crawls.columns = ['crawl']
        crawl_iloc = all_crawls[all_crawls['crawl'] == crawl].index[0]
        all_crawls = all_crawls.iloc[crawl_iloc:]
        for crawl in all_crawls['crawl']:
            with open(save_path, 'a') as f:
                f.write(f'{url},{crawl}\n')
        return len(all_crawls)





def scrape_crawl(crawl, url):
    encoded_url = urllib.parse.quote(url, safe='')
    resp = requests.get(
        f'http://index.commoncrawl.org/{crawl}-index?url={encoded_url}&output=json')
    if resp.status_code != 200:
        return log_completed_crawl(url, crawl=crawl, was_successful=False)
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
            return log_completed_crawl(url, crawl=crawl, was_successful=False)

    save_htmls(htmls, url, crawl)
    return log_completed_crawl(url, crawl=crawl, was_successful=True)



def scrape_current(url):
    try:
        resp = requests.get(url)
    except requests.exceptions.ConnectionError:
        return log_completed_crawl(url, was_successful=False, cc_or_current='current')

    if resp.status_code != 200:
        return log_completed_crawl(url, was_successful=False, cc_or_current='current')

    htmls = [resp.content]
    save_htmls(htmls, url, cc_or_current='current', crawl=None)
    return log_completed_crawl(url, cc_or_current='current', was_successful=True)



if __name__ == '__main__':
    # Find urls that need to be scraped
    urls = read_url_data()


    # Find list of url/crawls combos already searched
    already_completed = load_crawls_to_search(cc_or_current='current')
    to_search = urls.merge(already_completed, how='left', left_on=['bio_url'], right_on=['url'], indicator=True)
    to_search = to_search[to_search['_merge'] == 'left_only']
    to_search = to_search.drop(columns=['_merge'])

    with tqdm(total=len(to_search), smoothing=0) as pbar:
        for url in to_search['bio_url'].values:
            completed = scrape_current(url)
            pbar.update(completed)