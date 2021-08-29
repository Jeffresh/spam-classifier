from load_data import *
from get_data import *

if __name__ == '__main__':

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
