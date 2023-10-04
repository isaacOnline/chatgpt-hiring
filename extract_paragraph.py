import os
import re

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

def extract_paragraph(html, url, crawl):
    soup = BeautifulSoup(html, 'html.parser')
    paragraphs = [p.get_text() for p in soup.find_all('p')]
    professions = ['attorney','paralegal','surgeon','physician','professor','teacher']
    pattern = '[A-Z][a-zA-Z]+ [A-Z][a-zA-Z]+ is an? [A-Za-z]* ?(' + '|'.join(professions) + ')'
    matches = [p for p in paragraphs if re.match(pattern, p)]
    if len(matches) == 0:
        print(f'Found {len(matches)} matches for {url}')
        return None
    else:
        match_info = []
        for m in matches:
            m = re.sub('\s+', ' ', m)
            m = m.replace('\xa0', ' ')
            m = m.replace('\\n', ' ')
            m = m.replace('\\t', ' ')
            m = m.replace('\\r', ' ')
            m = m.replace('  ', ' ')
            m = m.strip()

            pronouns = ['he', 'she', 'they', 'him', 'her', 'them']
            pronoun_pattern = '(\s|^|\.|,|;|:|\?|!|\'|\"|\(|\))(' + r'|'.join(pronouns) + r')(\s|$|\.|,|;|:|\?|!|\'|\"|\(|\))'
            observed_pronouns = re.findall(pronoun_pattern, m, flags=re.IGNORECASE)
            observed_pronouns = np.unique([p[1].lower() for p in observed_pronouns])
            gender = 'male' if all([p in ['he', 'him'] for p in observed_pronouns]) else 'unsure'
            gender = 'female' if all([p in ['she', 'her'] for p in observed_pronouns]) else gender

            professions = ['attorney', 'paralegal', 'surgeon', 'physician', 'professor', 'teacher']
            name_pattern = '([A-Z][a-zA-Z]+) ([A-Z][a-zA-Z]+) is an? [A-Za-z]* ?(' + '|'.join(professions) + ')'
            names = re.findall(name_pattern, m)
            fnames = [n[0] for n in names]
            lnames = [n[1] for n in names]
            professions = [n[2] for n in names]
            sentences = m.split('.')
            test_paragraph = []
            declaratory_sentences = []
            for s in sentences:

                if not re.match(name_pattern, s):
                    # remove name
                    for fname in fnames:
                        s = s.replace(fname, 'FIRST_NAME')
                    for lname in lnames:
                        s = s.replace(lname, 'LAST_NAME')
                    # remove profession
                    for profession in professions:
                        s = s.replace(profession, 'PROFESSION')
                    test_paragraph.append(s)
                else:
                    declaratory_sentences.append(s)

            test_paragraph = '.'.join(test_paragraph)

            match_info.append(pd.DataFrame(
                {'url': url, 'crawl': crawl, 'paragraph': m, 'test_paragraph': test_paragraph,
                'declaratory_sentences': declaratory_sentences, 'profession':profession,'gender': [gender]}))
    match_info = pd.concat(match_info).drop_duplicates().reset_index(drop=True)
    return match_info


if __name__ == '__main__':
    for cc_or_current in ['cc', 'current']:
        htmls = pd.read_csv(os.path.join('processed_data',f'{cc_or_current}_htmls.csv'))
        paragraphs = []
        for _, html_info in htmls.iterrows():
            html = html_info['html']
            url = html_info['url']
            if cc_or_current == 'cc':
                crawl = html_info['crawl']
            else:
                crawl = None
            p = extract_paragraph(html, url, crawl)
            if p is not None:
                paragraphs.append(p)
        paragraphs = pd.concat(paragraphs).drop_duplicates().reset_index(drop=True)
        save_path = os.path.join('processed_data',f'{cc_or_current}_paragraphs.csv')
        paragraphs.to_csv(save_path, index=False)

