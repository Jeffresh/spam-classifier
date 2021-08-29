import email
import email.policy
import os
from concurrent import futures


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
