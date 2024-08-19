import json
import os
import pandas as pd

class ReadDB:

    def __init__(self, db_dir='./../data/',
                 book_schema={'title': '', 'author': '', 'city': '', 'country': '', 'page': ''}):
        self.db_dir = db_dir
        self.book_schema = book_schema

    def run(self):
        self.clean_db()

    def clean_db(self):
        booklist = os.listdir(self.db_dir)
        booklist.sort()

        self.book_dicts = {}
        self.failed_check = []
        self.failed_schema = {}

        # only allow json
        self.booklist = [book for book in booklist if book.split('.')[-1] == 'json']  # ensures book ends in .json

        for book in self.booklist:
            self.check_book(book)

        self.clean_db_df = pd.DataFrame(list(self.book_dicts.values()))

    def check_book(self, book):

        book_file = self.db_dir + book
        book_data = self.get_book_data(book_file)
        if isinstance(book_data, dict):
            #self.book_dicts[book] = book_data
            #"""
            if book_data.keys() == self.book_schema.keys():
                self.book_dicts[book] = book_data
            else:
                self.failed_check.append(book)
                self.failed_schema[book] = book_data
            #"""
        else:
            self.failed_check.append(book)

    @staticmethod
    def get_book_data(book_file):

        with open(book_file, 'r') as f:
            data = json.load(f)

        return data