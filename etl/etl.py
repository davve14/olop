import configparser
import glob
import os
import tempfile
import time
from logging import config
from os import listdir
from os.path import isfile, join
import gdown
import requests
import logging
import csv
import validators
import pandas as pd
import numpy as np
from db import db
import re
import json
from pdf2image import convert_from_path, convert_from_bytes
try:
    from PIL import Image
except ImportError:
    import Image
import pytesseract

database = db.Db('db/db.json')


class DataIngestor:

    def __init__(self):
        self.log_level = "WARNING"
        self.doc_id = ""
        self.doc_sheet = ""
        self.docs_file = ""
        self.get_config()
        self.logger = self.create_logger()
        self.logger.debug("Creating an instance of DataGenerator")

    def get_config(self):
        datagen_config = configparser.ConfigParser()
        datagen_config.read("etl.conf")
        self.doc_id = datagen_config['DATA_INGEST']['doc_id']
        self.doc_sheet = datagen_config['DATA_INGEST']['doc_sheet']
        self.log_level = datagen_config['DATA_INGEST']['log_level']
        self.docs_file = datagen_config['DATA_INGEST']['docs_file']

    def create_logger(self):
        if not os.path.exists("log"):
            os.mkdir("log")
        data_ingest_logger = logging.getLogger("DataIngestor")
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        fh = logging.FileHandler("log/data_ingest.log", "a")
        fh.setFormatter(formatter)
        data_ingest_logger.setLevel(self.log_level)
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        data_ingest_logger.addHandler(fh)
        data_ingest_logger.addHandler(ch)
        return data_ingest_logger

    def download_docs_sheet(self):
        self.logger.info("Downloading docs sheet")
        url = f"https://docs.google.com/spreadsheets/d/{self.doc_id}/gviz/tq?tqx=out:csv&sheet={self.doc_sheet}"
        r = requests.get(url, allow_redirects=True)
        csv_file = open(self.docs_file, "wb")
        csv_file.write(r.content)
        csv_file.close()

    def build_download_list(self):
        self.logger.info(f"Parsing and validating URLs from DocsSheet")
        docs = []
        with open(self.docs_file, newline='') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',', quotechar='"')
            for row in csv_reader:
                if validators.url(row[6].strip().replace("\n", "")):
                    doc = dict()
                    doc["number"] = row[0]
                    doc["name"] = row[1]
                    doc["update_date"] = row[3]
                    doc["anmarkning"] = row[4]
                    doc["url"] = row[6]
                    docs.append(doc)

        docs_file = pd.DataFrame(docs)
        data = []
        df1 = pd.DataFrame(data, columns=['url'])
        df2 = pd.DataFrame(database.get_documents())
        existing_docs = pd.concat([df1, df2])
        new_docs = existing_docs.merge(docs_file,
                                       on='url',
                                       how='outer',
                                       indicator=True).loc[lambda x: x['_merge'] == 'right_only']
        return new_docs

    def download_docs(self):
        docs = self.build_download_list()
        for index, doc in docs.iterrows():
            url = create_download_link(doc['url'])['url']
            done = False
            while done is not True:
                try:
                    filename = gdown.download(url, "docs" + "/", quiet=False, fuzzy=True)
                    if filename is None:
                        print("waiting...")
                        time.sleep(600)
                        continue
                    else:
                        database.insert_document({'url': doc['url']})
                        done = True
                except RuntimeError as e:
                    print(e)


class DataTransformer:

    def __init__(self):
        etl_config = configparser.ConfigParser()
        etl_config.read("etl.conf")
        self.input_folder = etl_config['DATA_TRANSFORM']['input_folder']
        self.ppm_folder = etl_config['DATA_TRANSFORM']['ppm_folder']
        self.text_folder = etl_config['DATA_TRANSFORM']['text_folder']

    def convert_pdf_to_ppm(self):
        if not os.path.exists("tmp"):
            os.mkdir("tmp")
        if not os.path.exists(self.ppm_folder):
            os.mkdir(self.ppm_folder)
        pdfs = glob.glob(self.input_folder + "/*.pdf")
        for pdf in pdfs:
            with tempfile.TemporaryDirectory("", "tmp", "./tmp") as temp_path:
                pages = convert_from_path(pdf, dpi=300, thread_count=4, output_folder=temp_path)
                for page in pages:
                    page.save("%s-page%d.ppm" % (self.ppm_folder + "/" + get_file_base(pdf), pages.index(page)), "PPM")

    def convert_ppm_to_text(self):
        ppms = glob.glob(self.ppm_folder + "/*.ppm")
        for ppm in ppms:
            ppm_to_string(ppm, self.text_folder)


#Utils
def get_file_base(path):
    return os.path.basename(path).split(".")[0]


def ppm_to_string(ppm_file, output_dir):
    r = pytesseract.image_to_string(Image.open(ppm_file), lang='swe')
    filename = get_file_base(ppm_file)
    txt_file = open(output_dir + "/" + filename + ".txt", "w")
    txt_file.write(r)
    txt_file.close()

def fetch_file_metadata_from_google_drive(document_id):
    url = "https://drive.google.com/uc?id=" + document_id + "&authuser=0&export=download&confirm=t"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
    }
    verify = True
    session = requests.Session()
    meta = {'id': id, 'valid': False, 'filename': '', 'url': url}

    response = session.get(url, allow_redirects=True, headers=headers, stream=True, verify=verify)
    for k in response.headers:
        meta[k] = str(response.headers[k])

    if 'Content-Disposition' in response.headers:
        match = re.search(r'filename="(?P<filename>.+)"', response.headers['Content-Disposition'])
        remove_characters = dict((ord(char), None) for char in '/')
        dest_path = match['filename'].translate(remove_characters)
        meta['filename'] = dest_path
        meta['valid'] = True
    return meta['filename']


def download_docs_sheet(key, sheet, output_dir):

    url = f"https://docs.google.com/spreadsheets/d/{key}/gviz/tq?tqx=out:csv&sheet={sheet}"
    r = requests.get(url, allow_redirects=True)
    csv_file = open(output_dir + "/" + "docs.csv", "wb")
    csv_file.write(r.content)
    csv_file.close()


def read_docs_sheet(filepath):
    logging.info(f"Parsing and validating URLs from DocsSheet")
    docs = []
    with open(filepath, newline='') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',', quotechar='"')
        for row in csv_reader:
            if validators.url(row[6].strip().replace("\n", "")):
                doc = dict()
                doc["number"] = row[0]
                doc["name"] = row[1]
                doc["update_date"] = row[3]
                doc["anmarkning"] = row[4]
                doc["url"] = row[6]
                docs.append(doc)
    return docs


def build_download_list(docs):
    docs = pd.DataFrame(read_docs_sheet(docs))
    data = []
    df1 = pd.DataFrame(data, columns=['url'])
    df2 = pd.DataFrame(database.get_documents())
    existing_docs = pd.concat([df1, df2])
    new_docs = existing_docs.merge(docs,
                                on='url',
                                how='outer',
                                indicator=True).loc[lambda x: x['_merge'] == 'right_only']
    return new_docs


def rebuild_index_db(folder, docs):
    existing_files = load_existing_files(folder)
    for doc in docs:
        url = create_download_link(doc['url'])['url']
        docid = create_download_link(doc['url'])['id']
        try:
            meta = fetch_file_metadata_from_google_drive(docid)
            # database.insert_document({'url': url, 'document': meta})
        except Exception as e:
            print(e)


def download_docs(output_folder, docs):
    for index, doc in docs.iterrows():
        url = create_download_link(doc['url'])['url']
        # docid = createDownloadLink(doc['url'])['id']
        done = False
        while done is not True:
            try:
                filename = gdown.download(url, output_folder + "/", quiet=False, fuzzy=True)
                if filename is None:
                    print("waiting...")
                    time.sleep(600)
                    continue
                else:
                    database.insert_document({'url': doc['url']})
                    done = True
            except RuntimeError as e:
                print(e)


def create_download_link(url):
    # url_prefix = "https://drive.google.com/uc?id="
    url_prefix = 'https://drive.google.com/file/d/'
    url_suffix = '/view?usp=sharing'
    url1 = url.find("file/d/")
    url2 = url[url1 + 7:]
    doc_id = url2[:url[url1 + 7:].find("/")]
    print(url_prefix + doc_id + url_suffix)
    return {'url': url_prefix + doc_id + url_suffix, 'id': doc_id}


def load_existing_files(folder):
    files = [f for f in listdir(folder) if isfile(join(folder, f))]
    pdf_files = filter(lambda f: f.endswith(('.pdf', '.PDF')), files)
    df = pd.DataFrame(np.array(list(pdf_files)), columns=['filename'])
    return df
