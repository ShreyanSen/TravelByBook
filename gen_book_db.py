

from src.wiki import FetchBookerPages, FetchWikiHNPages, WikiRead
from src.db import GeoBookJSONtoCSV
import json
import time
import os
import argparse


def get_args():
    """
    source: the wiki page source for compiling a set of books to query and geolocate, is a json 

    """
    parser = argparse.ArgumentParser(description='Arguments for getting device adjusted and unadjusted segment std')
    parser.add_argument('-pagelist-source', type=str, help='specifies sources from which we generate a pagelist, can take values booker,'
                                                  'whn,  or all', default=None)
    parser.add_argument('-pagelist-file', type=str, help='instead of generating a pages json this points to a file,'
                        'we load the json at this string and then pass the list on', default=None)

    parser.add_argument('-gen-json-data', type=bool, help = 'set to True if generating json database of classified books from pagelist', default=True)
    parser.add_argument('-gen-csv-db', type=bool, help='set to True if generating csv database', default=False)
    parser.add_argument('-update-csv-db', type=bool, help='set to True if updating csv database manually', default=False)
    args = parser.parse_args()
    return args.pagelist_source, args.pagelist_file, args.gen_json_data, args.gen_csv_db, args.update_csv_db


class GBTaskManager:

    """
    Class that manages tasks, entry point for all the database generation and maintenance work.

    From here, you can generate a pagelist file (file specifying wikipedia articles to summarize), load
    a previously generated pagelist file, generate the json database of classified books from a pagelist,
    or generate or update a csv file compiling and cleaning the json database for consumption by the front end


    """
    def __init__(self, pagelist_source, pagelist_file, gen_json_data = True, gen_csv_db = False, update_csv_db = False, json_output_dir='data/json/', geocoded_csv_output_dir='data/csv/'):

        """
        pagelist_source: string ('whn','booker','all') specifying a source from which to build a pagelist of wikipedia pages to query and process
        pagelist_file: string specificying location of pagelists (data/pagelists/something...) for if we want to load rather than generate pagelist
        gen_json_data: boolean specifying whether we want to generate json data for a given pagelist. Default to true means
            that specifying pagelist_source or pagelist_file automatically results in the generation of json data.
        gen_csv_db: boolean speciying whether we want to generate the csv database file from summarized wiki pages json
        update_csv_db: flag to just run the update book df function. use it if you want to modify book df
            (by making manual changes to the GeoBookJSONtoCSV.correct_book_df_errors function) but don't
            need to rerun geocoding / the entire json to csv pipeline
        json_output_dir: directory where json data of classified books is written
        geocoded_csv_output_dir: directory where csv file of book db is written
        """
        self.gen_source = pagelist_source
        self.load_source = pagelist_file
        self.gen_json_data = gen_json_data
        self.gen_csv_db = gen_csv_db
        self.update_csv_db = update_csv_db

        # remnants from a more flexible model--can be used to add in flexibility for where we read/write db data
        # currently hardcoded to read/write from standard data directory structure
        self.json_output_dir = json_output_dir
        self.geocoded_csv_output_dir = geocoded_csv_output_dir

    def run_tasks(self):

        if self.gen_source == 'booker':
            self.run_booker()
        elif self.gen_source == 'whn':
            self.run_wiki_hist_novels()
        elif self.gen_source == 'all':
            self.run_all()
        
        if self.load_source:
            self.run_filtered_page_list(self.load_source)

        if (self.gen_csv_db or self.update_csv_db):
            self.build_geolocated_csv()
        

    def run_booker(self):

        wf = FetchBookerPages()
        wf.run()

        if self.gen_json_data:
            self.geolocate_books(wf.booker_eligible_pages)
        return

    def run_wiki_hist_novels(self):
        hnp = FetchWikiHNPages()
        # hnp.run_full_pipeline() not in use because produces low quality results
        hnp.triple_filter_short_pipeline() # because of quality issues we need this "triple filter" run, see class for more
        # note, this triple filter process takes a long time
        if self.gen_json_data:
            self.geolocate_books(hnp.filtered_pages)

    def run_filtered_page_list(self, fpg_file: str):
        """
        If we already have a filtered page list, for example from a run_wiki_hist_novels
        fpg_file: directory and file to get to json of wiki pages list you want to load. ex: data/whn_pages/triple_filtered_pages.json
        """

        with open(fpg_file, 'r') as f:
            fpg = json.load(f)
        if self.gen_json_data:
            self.geolocate_books(fpg)


    def geolocate_books(self, eligible_pages: list):

        wr = WikiRead()  # pass in a wikipediaapi object
        num_books = len(eligible_pages)
        for i in range(0, num_books):
            book = eligible_pages[i]

            # first, check if the file already exists, if it does skip
            output = self.json_output_dir+book+'.json' # this should be in a class and we shouldn't hardcode this output dir
            if os.path.isfile(output):
                continue

            print("processing book num " + str(i+1)+"/"+str(num_books)+ ": "+ book + " at " + time.ctime())
            book_data = wr.pull_book(book)

            with open(output, 'w') as f:
                json.dump(book_data, f)
        return


    def run_all(self):
        self.run_booker()
        self.run_wiki_hist_novels()
        return
    
    def build_geolocated_csv(self):
        """
        takes in json files (where book data are stored) and writes a csv where those
        files are compiled into one dataframe, and also geolocates the results from those
        files and tries to create a sensible and consistent lat / lon / address estimate (or NaNs)

        activates if passed a location to write csv

        note: only triggers if both a data_dir and a csv_out dir are passed in
        however by default data_dir is passed in, so effectively it will trigger if 
        the geocoded_csv_output_dir is passed in via flag and it will try at the default
        expected json_output_dir (folder where it pulls json data files to construct a 
        final csv data file) but will fail if there is nothing there
        """
        j2csv = GeoBookJSONtoCSV(data_dir = self.json_output_dir, db_dir = self.geocoded_csv_output_dir)
        if self.gen_csv_db:
            j2csv.run_all()
        if self.update_csv_db:
            print('running update')
            j2csv.update_book_df()

if __name__ == '__main__':
    pagelist_source, pagelist_file, gen_json_data, gen_csv_db, update_csv_db = get_args()
    tasks = GBTaskManager(pagelist_source, pagelist_file, gen_json_data, gen_csv_db, update_csv_db)
    tasks.run_tasks()
