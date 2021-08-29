import os
import requests
from urllib.request import urlopen, urlretrieve
from bs4 import BeautifulSoup
import collections
import pathlib
import tarfile
from concurrent import futures


TYPES = ['easy', 'hard', 'spam']

url = "https://spamassassin.apache.org/old/publiccorpus/"

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_file_names(url: str) -> iter:
    response = urlopen(url)
    soup_wrap = BeautifulSoup(response.read())
    return (file.contents[0] for file in soup_wrap.find_all('a') if '.' in file.contents[0])


def get_order_files_per_type(types: list, file_names: list) -> tuple:
    files = collections.defaultdict(list)
    other_files = []

    for file in file_names:
        if any((one_type in file for one_type in types)):
            for allowed_type in types:
                if allowed_type in file:
                    files[allowed_type].append(file)
                    break
        else:
            other_files.append(file)

    return other_files, files


def get_updated_file_names(types: list, files: dict) -> dict:

    files_updated = collections.defaultdict(list)

    for allowed_type in types:
        for file in files[allowed_type]:
            file_part = file.split('_')
            if '2.' in file_part[-1]:
                files_updated[allowed_type+'_2'] = file
            else:
                if len(files_updated[allowed_type]) > 0:
                    date_prev = int(files_updated[allowed_type].split('_')[0])
                    date_actual = int(file_part[0])
                    if date_actual > date_prev:
                        files_updated[allowed_type] = file
                else:
                    files_updated[allowed_type] = file

    return files_updated


def download_file(url_file, dest_path):
    r = requests.get(url_file)
    file_name = os.path.basename(url_file)
    dest_pathfile = os.path.join(dest_path, file_name)
    with open(dest_pathfile, 'wb') as f:
        f.write(r.content)
        file = tarfile.open(dest_pathfile)
        file.extractall(os.path.join(dest_path))

    return True


def download_many(args):
    workers = min(20, len(args))
    with futures.ThreadPoolExecutor(workers) as executor:
        res = executor.map(lambda params: download_file(*params), sorted(args))
    return len(list(res))


def collect_update_data():
    file_names = list(get_file_names(url))
    other_files, files = get_order_files_per_type(TYPES, file_names)
    files_names_updated = get_updated_file_names(TYPES, files)

    data_path = os.path.abspath(os.path.join(ROOT_DIR, 'data'))
    print(data_path)

    if not os.path.exists(data_path):
        os.makedirs(data_path)

    args = [(os.path.join(url, value), data_path)
            for value in files_names_updated.values()]
    download_many(args)


# if __name__ == '__main__':

#     file_names = list(get_file_names(url))
#     other_files, files = get_order_files_per_type(TYPES, file_names)
#     files_names_updated = get_updated_file_names(TYPES, files)

#     data_path = os.path.abspath(os.path.join(ROOT_DIR, 'data'))
#     print(data_path)

#     if not os.path.exists(data_path):
#         os.makedirs(data_path)

#     args = [(os.path.join(url, value), data_path)
#             for value in files_names_updated.values()]
#     download_many(args)
