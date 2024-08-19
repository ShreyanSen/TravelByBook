

from src.wiki import FetchBookerPages, FetchWikiHNPages, WikiRead
from src.db import GeoBookJSONtoCSV
import json
import time
import os
import argparse


def get_args():
    """
    source: the wiki page source for compiling a set of books to query and geolocate
    """
    parser = argparse.ArgumentParser(description='Arguments for getting device adjusted and unadjusted segment std')
    parser.add_argument('-gen-source', type=str, help='specifies sources from which we gen db, can take values booker,'
                                                  'whn,  or all', default=None)
    parser.add_argument('-load-source-file', type=str, help='instead of generating a pages json this points to a file,'
                        'we load the json at this string and then pass the list on', default=None)

    parser.add_argument('-json-output-dir', type=str, description = 'output directory for json files', default='data/json/')
    parser.add_argument('-geocoded-csv-output-dir', type=str, description='pass location of csv to write if you want to read book json files,'
                        ' geocode them if possible, and return clean address and latlons,' 
                        'also make sure -json-output-dir is passed or default is ok because it pulls jsons from there', default=None)
    args = parser.parse_args()
    return args.gen_source, args.load_source_file, args.json_output_dir, args.geocoded_csv_output_dir


class GBTaskManager:

    def __init__(self, gen_source, load_source, json_output_dir, geocoded_csv_output_dir):

        self.gen_source = gen_source
        self.load_source = load_source
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

        if self.geocoded_csv_output_dir:
            self.build_geolocated_csv()

    def run_booker(self):

        wf = FetchBookerPages()
        wf.run()

        self.geolocate_books(wf.booker_eligible_pages)
        return

    def run_wiki_hist_novels(self):
        hnp = FetchWikiHNPages()
        # hnp.run_full_pipeline() not in use because produces low quality results
        hnp.triple_filter_short_pipeline() # because of quality issues we need this "triple filter" run, see class for more
        # note, this triple filter process takes a long time

        self.geolocate_books(hnp.filtered_pages)

    def run_filtered_page_list(self, fpg_file: str):
        """
        If we already have a filtered page list, for example from a run_wiki_hist_novels
        fpg_file: directory and file to get to json of wiki pages list you want to load. ex: data/whn_pages/triple_filtered_pages.json
        """

        with open(fpg_file, 'r') as f:
            fpg = json.load(f)
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
        j2csv = GeoBookJSONtoCSV(data_dir = self.json_output_dir, csv_out = self.geocoded_csv_output_dir)
        j2csv.run_all()

if __name__ == '__main__':
    gen_source, load_source, json_output_dir, geocoded_csv_output_dir= get_args()

    tasks = GBTaskManager(gen_source, load_source, json_output_dir, geocoded_csv_output_dir)
    tasks.run_tasks()
