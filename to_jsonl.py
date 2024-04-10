import json

import pandas as pd
from bs4 import BeautifulSoup
import os


def get_files():
    root, _, files = next(os.walk('dice'))
    return [os.path.join(root, file) for file in files]


def get_soup(path):
    soup = None
    for encoding in ['utf-16le', 'latin-1']:
        try:
            with open(path, 'r', encoding=encoding) as file:
                soup = BeautifulSoup(file.read(), 'html.parser', from_encoding=encoding)
        except Exception as e:
            continue
        else:
            return soup
    return soup


if __name__ == '__main__':
    data = []
    for path in get_files():
        soup = get_soup(path)
        if soup is None:
            continue
        elem = soup.find('div', {'data-testid': 'jobDescriptionHtml'})
        if not elem:
            continue
        text = ''
        for string in elem.strings:
            if string != ' ':
                text += string + '\n'
        elem = soup.find('div', {'data-testid': 'skillsList'})
        if elem:
            for string in elem.strings:
                if string != ' ':
                    text += string + '\n'
        data.append({
            'id': path.split('\\')[-1].split('.')[0],
            'text': text,
            'title': soup.find('h1', {'data-cy': 'jobTitle'}).text
        })
    pd.DataFrame(data).to_json('job_collection.jsonl', orient='records', lines=True, index=False)
