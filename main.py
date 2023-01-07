import spacy
from spacy.lang.sv.examples import sentences
from etl import etl
import os.path
from db import db


def run_code():

    #etl.downloadDocsSheet("1O37mhN5bMt5nd-CaO7ue_3KMbip6eVETWKXwfILsf3E", "Beställt", "docs")
    #etl.downloadDocs("docs", etl.readDocsSheet("docs/docs.csv"))
    #print(etl.readDocsSheet("sheet/docs.csv"))
    #etl.loadExistingFiles("docs")


    # nlp = spacy.load("sv_core_news_md")
    # doc = nlp(sentences[0])
    # print(doc.text)
    # for token in doc:
    #     print(token.text, token.pos_, token.dep_)

    database = db.Db('db/db.json')
    # database.insert_document({'url': '', 'document': ''})


    #etl.rebuild_index_db("docs", etl.readDocsSheet("docs/docs.csv"))
    #etl.downloadDocsSheet("1O37mhN5bMt5nd-CaO7ue_3KMbip6eVETWKXwfILsf3E", "Beställt", "sheet")
    #etl.download_docs("docs", etl.build_download_list("sheet/docs.csv"))
    #etl.build_download_list("docs/docs.csv")

    data_ingestor = etl.DataIngestor()
    data_ingestor.download_docs_sheet()
    data_ingestor.download_docs()
    #data_transformer = etl.DataTransformer()
    #data_transformer.convert_ppm_to_text()


if __name__ == '__main__':
    run_code()
