from collections import Counter
import email
import email.policy
import os
import numpy as np
from concurrent import futures

import re
from html import unescape

try:
    ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except Exception:
    ROOT_DIR = os.path.abspath('..')

data_path = os.path.join(ROOT_DIR, 'data')


def get_folder_names(data_path):
    return [os.path.join(data_path, file_name)
            for file_name in os.listdir(data_path)
            if os.path.isdir(os.path.join(data_path, file_name))]


def get_files(folder_path: str) -> iter:
    return (file for file in os.listdir(folder_path))


def retrieve_email(file_path: str) -> 'bytes':
    with open(file_path, 'rb') as file:
        return email.parser.BytesParser(policy=email.policy.default).parse(file)


def load_one_data(data_folder_path: str) -> list:
    return [retrieve_email(os.path.join(data_folder_path, file)) for file in get_files(data_folder_path)]


def load_data(data_folder_path: list) -> dict:
    emails = {}
    with futures.ThreadPoolExecutor(len(data_folder_path)) as executor:
        res = executor.map(load_one_data, data_folder_path)

    for folder_path, mails in zip(data_folder_path, res):
        folder_name = os.path.basename(folder_path)
        emails[folder_name] = mails

    return emails


def get_structures(email):
    if isinstance(email, str):
        return email
    payload = email.get_payload()
    if isinstance(payload, list):
        return "multiapart({})".format(', '.join([get_structures(sub_email)
                                                  for sub_email in payload]))
    else:
        return email.get_content_type()


def structures_counter(emails):
    structures = Counter()
    for email in emails:
        structure = get_structures(email)
        structures[structure] += 1

    return structures


def html_to_plain_text(html):
    text = re.sub('<head.*?>.*?</head>', '', html, flags=re.M | re.S | re.I)
    text = re.sub('<a\s.*?>', ' HYPERLINK ', text, flags=re.M | re.S | re.I)
    text = re.sub('<.*?>', '', text, flags=re.M | re.S)
    text = re.sub(r'(\s*\n)+', '\n', text, flags=re.M | re.S)
    return unescape(text)


def email_to_text(email):
    html = None
    for part in email.walk():
        ctype = part.get_content_type()
        if not ctype in ("text/plain", "text/html"):
            continue
        try:
            content = part.get_content()
        except:  # in case of encoding issues
            content = str(part.get_payload())
        if ctype == "text/plain":
            return content
        else:
            html = content
    if html:
        return html_to_plain_text(html)


def create_dataset(emails: dict) -> np.array:
    y = []
    mails = []

    for key, values in emails.items():
        v = [1]
        if 'ham' in key:
            v = [0]

        y += v * len(values)

        mails += values

    return np.array(mails, object), np.array(y)
