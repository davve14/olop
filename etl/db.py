from tinydb import TinyDB, Query

class Db:
    def __init__(self, path):
        self.path = path
        self.db = TinyDB(path)
        self.db.document = self.db.table('document')

    def insert_document(self, row):
        self.db.document.insert(row)

    def read_document(self, url):
        document = Query()
        self.db.search(document.url == url)

    def get_documents(self):
        documents = self.db.document.all()
        return documents

