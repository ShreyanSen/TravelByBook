
import wikipediaapi
from langchain_community.llms import Ollama
import pandas as pd # library for data analysis
import requests # library to handle requests
from bs4 import BeautifulSoup # library to parse HTML documents
import time
import json
import os


class WikiClassify:
    """
    Classify wikipedia articles
    """

    def __init__(self, w_page):
        self.llm = Ollama(model="llama3")
        self.wiki = wikipediaapi.Wikipedia('MyProjectName (merlin@example.com)', 'en')
        self.w_page = w_page
        self.query_summary()

    def query_summary(self):
        self.w_summary = self.wiki.page(self.w_page).summary
        self.w_summary_to_first_period = self.w_summary.split('.')[0]
        self.w_summary_to_first_comma = self.w_summary.split(',')[0]

    def classify_page_to_first_period(self):
        self.classify_page_by_w_text(w_text=self.w_summary_to_first_period)

    def classify_page_to_first_comma(self):
        self.classify_page_by_w_text(w_text=self.w_summary_to_first_comma)

    def classify_page_by_summary(self):
        self.classify_page_by_w_text(w_text=self.w_summary)

    def classify_page_by_w_text(self, w_text):
        """
        For a wikipedia page, classify it as belonging to a novel, an author, or neither ("other")
        """

        output1 = '<result>\n{\n"class": "author"\n}\n</result>'
        output2 = '<result>\n{\n"class": "book"\n}\n</result>'
        output3 = '<result>\n{\n"class": "other"\n}\n</result>'

        prompt = "You are a bot that reads wikipedia articles about books and returns information about them. Your output must be in valid JSON. \
        Do not output anything other than the JSON.\
        First, determine if the wikipedia page is a page for an author, a book, or something else.\
        If it is a page for an author (not a book), add the value author to the JSON with the key class. \
        If it is a page for a book (not an author), add the value book to the JSON with the key class. \
        If it is neither for a book nor an author, add the value other to the JSON with the key class \
        Finally, surround your JSON output with <result></result> tags. \
        \
        Ensure the JSON output has one key, class, and one value, which can either be author, book, or other.\
        Do not return any other type of output.\
        \
        Here is the text from the wikipedia article for this book: "

        end_prompt = "Based on the book's wikipedia article provided above, create a JSON describing this book. Your output must be in valid JSON. \
        Do not output anything other than the JSON.\
        First, determine if the wikipedia page is a page for an author, a book, or something else.\
        If it is a page for an author (not a book), add the value author to the JSON with the key class. \
        If it is a page for a book (not an author), add the value book to the JSON with the key class. \
        If it is neither for a book nor an author, add the value other to the JSON with the key class \
        \
        Ensure the JSON output has one key, class, and one value, which can either be author,book, or other.\
        Do not return any other type of output.\
        \
        Finally, surround your JSON output with <result></result> tags.\
        The three allowable outputs are: "

        self.llm_response = self.llm(prompt + w_text + end_prompt + output1 + "or " + output2 + "or " + output3)
        self.parse_response()

    def parse_response(self):
        json_string = self.llm_response.split('<result>')[1].split('</result>')[0]
        self.parsed_json = json.loads(json_string.strip())


class FetchWikiHNPages:
    """
    Going to get a nice clean set of book pages from this website: https://en.wikipedia.org/wiki/List_of_historical_novels

    Currently the way to use this class is as follows:
    hnp = FetchWikiHNPages()
    hnp.triple_filter_short_pipeline()

    the output, 'triple_filtered_pages.json' should contain a list of high quality wikipedia page titles going to books
    
    """

    def __init__(self, root_page="List_of_historical_novels", data_dir='data/whn_pages/'):
        self.llm = Ollama(model="llama3")
        self.wiki = wikipediaapi.Wikipedia('MyProjectName (merlin@example.com)', 'en')
        self.root_page = self.wiki.page(root_page)
        self.data_dir = data_dir

    def triple_filter_short_pipeline(self):
        """
        temp pipeline to get triple filtering running
        TODO: merge with run_full_pipeline
        note: inefficient because we're better off just running the pages we already single filtered

        
        """

        self.load_filtered_and_unfiltered_pages()
        self.triple_filter_pages()

    def run_full_pipeline(self, mode='continue'):
        """

        runs full pipeline
        TODO: write this so that we can tell where we are and if we failed jobs. Wherever state of completion we are when
        we have the list, we start from there and keep going. there could also be a setting for delete current records and start from
        scratch. So if we already have a filtered_pages_final.json just use that. that would look like:


        mode: takes on vals 'continue' to continue from last known point or 'overwrite' if we want to start from the beginning
        """

        if mode == 'continue':
            # use existence of files written during process to determine where we left off
            if os.path.exists(self.data_dir + 'filtered_pages_final.json'):  # means self.filter_pages() ran
                print('final page list already exists--loading...')
                with open(self.data_dir + 'filtered_pages_final.json', "r") as f:
                    self.filtered_pages = json.load(f)

            # elif os.path.exists(
                #    self.data_dir + 'filtered_pages_cache.json'):  # means self.filter_pages ran but did not complete...
                # hard case...because there are some edge cases, but let's right a solution that updates with cached results and continues
                # not really adequate because for instance filtered_pages_cache might update but unfiltered_pages_cache might not, in same loop
                # meaning they would be mismatched
                # actually...let's just leave this case for another day, I want to move on to generating actual data rather than writing endless cases

            # replace / reformat this elif and below with load_filtered_and_unfiltered_pages
            # note the get_unfiltered_pages() in the else is included in load_filtered_and_unfiltered_pages()
            elif os.path.exists(self.data_dir + 'filtered_pages.json'):  # means self.reduce_pages() ran
                with open(self.data_dir + "unfiltered_pages.json", "r") as f:
                    self.unfiltered_pages = json.load(f)
                with open(self.data_dir + "filtered_pages.json", "r") as f:
                    self.filtered_pages = json.load(f)
                self.filter_pages()
            else:

                self.get_unfiltered_pages()
                self.reduce_pages()
                self.filter_pages()

        elif mode == 'overwrite':

            self.get_unfiltered_pages()
            self.reduce_pages()
            self.filter_pages()

    def load_filtered_and_unfiltered_pages(self):

        if os.path.exists(self.data_dir + 'filtered_pages.json'):  # means self.reduce_pages() ran
            with open(self.data_dir + "unfiltered_pages.json", "r") as f:
                self.unfiltered_pages = json.load(f)
            with open(self.data_dir + "filtered_pages.json", "r") as f:
                self.filtered_pages = json.load(f)  

        else:
            self.get_unfiltered_pages()
            self.reduce_pages()

    def get_unfiltered_pages(self):
        """
        Queries pages from hns site and does a validity check
        """
        print('started querying unfiltered pages at ' + time.ctime())

        root_links = self.root_page.links  # gets a dictionary whose keys are all the linked pages, potentially including red links though
        self.unfiltered_pages = list(root_links.keys())

        self.unfiltered_pages = [pg for pg in self.unfiltered_pages if self.wiki.page(pg).exists()]
        self.filtered_pages = []
        print('ended querying unfiltered pages at ' + time.ctime())

    def reduce_pages(self):
        """
        We want to limit the number of API calls we'll make, so let's move any pages with novel in the title over to our filtered list.
        This isn't robust to all possible edge cases but looks good for the actual list we're dealing with here (based on direct inspection).
        """

        # manually parsed suffixes that identify whether a wiki page in our list is a novel or a non-novel
        novel_title_suffix_list = ['novel', 'historical novel', 'book', 'Uris novel', 'Seton novel', '1962 novel',
                                   'Prus novel', 'novel series', 'Foote novel', 'Gann novel', 'Mario Puzo novel',
                                   'Spanish novel', 'Sutcliff novel', 'Scott novel', 'Penman novel']
        not_novel_title_suffix_list = ['journalist', 'author', 'novelist', 'writer', 'abolitionist', 'World War II',
                                       'consul 218 BC', 'fictional character', '535-554', '1919–1922', '410',
                                       'Third Punic War', 'Polish history']
        novels_by_wiki_title = [pg for pg in self.unfiltered_pages if
                                pg.rstrip(')').split('(')[-1] in novel_title_suffix_list]
        not_novels_by_wiki_title = [pg for pg in self.unfiltered_pages if
                                    pg.rstrip(')').split('(')[-1] in not_novel_title_suffix_list]

        self.unfiltered_pages = list(
            set(self.unfiltered_pages) - set(novels_by_wiki_title) - set(not_novels_by_wiki_title))
        self.filtered_pages.extend(novels_by_wiki_title)

        # write results of this stage of pipeline
        with open(self.data_dir + "unfiltered_pages.json", "w") as f:
            json.dump(self.unfiltered_pages, f)
        with open(self.data_dir + "filtered_pages.json", "w") as f:
            json.dump(self.filtered_pages, f)

    def filter_pages(self):
        """
        Checks that a page exists and that it's the page for an actual author
        """
        print('started filtering pages at ' + time.ctime())

        self.unfiltered_pages_class_dict = {}  # just as a backup, record the llm-generated class in a dict
        self.unfiltered_remaining = self.unfiltered_pages.copy()
        for pg in self.unfiltered_pages:
            self.wc = WikiClassify(w_page=pg)
            print('filtering page:  ' + str(pg) + ' at ' + time.ctime())

            self.wc.classify_page_by_summary()
            self.unfiltered_pages_class_dict[pg] = self.wc.parsed_json['class']
            if self.wc.parsed_json['class'] == 'book':
                self.filtered_pages.append(pg)
                with open(self.data_dir + "filtered_pages_cache.json", "w") as f:
                    json.dump(self.filtered_pages, f)

                self.unfiltered_remaining.remove(pg)
                with open(self.data_dir + "unfiltered_pages_cache.json", "w") as f:
                    json.dump(self.unfiltered_remaining, f)

        with open(self.data_dir + "filtered_pages_final.json", "w") as f:
            json.dump(self.filtered_pages, f)


    def triple_filter_pages(self):
        """
        Checks that a page exists and that it's the page for an actual author
        Uses three filters: classifying the summary page, classifying the text up to the first period, and up to the first comma.
        We use a triple filter because we saw that just classifying based on the summary text yielded too much misclassification
        Instead, just pass through pages that are classified as books by all three 
        """
        print('started filtering pages at ' + time.ctime())
        ## TODO: need to manage self.filtered_pages and self.unfiltered_pages and connect it to pipeline
        self.triple_filter_results = {}
        for pg in self.unfiltered_pages:
            self.wc = WikiClassify(w_page=pg)
            print('filtering page:  ' + str(pg) + ' at ' + time.ctime())

            self.wc.classify_page_by_summary()
            first_pass = self.wc.parsed_json['class']

            self.wc.classify_page_to_first_period()
            second_pass = self.wc.parsed_json['class']

            self.wc.classify_page_to_first_comma()
            third_pass = self.wc.parsed_json['class']

            self.triple_filter_results[pg] = {'first_pass': first_pass, 'second_pass':second_pass, 'third_pass':third_pass}

            if (first_pass=='book') & (second_pass=='book') & (third_pass=='book'):
                self.filtered_pages.append(pg)


        with open(self.data_dir + "triple_filtered_pages.json", "w") as f:
            json.dump(self.filtered_pages, f)

        with open(self.data_dir + "triple_filtered_pages_verbose.json", "w") as f:
            json.dump(self.triple_filter_results, f)


class FetchBookerPages:

    def __init__(self):
        self.wiki = wikipediaapi.Wikipedia('MyProjectName (merlin@example.com)', 'en')


    def run(self):

        self.get_booker_table()
        self.xref_booker_pages()

    def get_booker_table(self):
        """
        Gets a dataframe with the list of all booker prize books

        """
        self.booker_page = 'List_of_winners_and_nominated_authors_of_the_Booker_Prize'
        self.booker_url = 'https://en.wikipedia.org/wiki/' + self.booker_page

        wikiurl =self.booker_url
        table_class ="wikitable sortable jquery-tablesorter"
        response =requests.get(wikiurl)
        print(response.status_code)

        self.soup = BeautifulSoup(response.text, 'html.parser')
        bookertable =self.soup.find('table' ,{'class' :"wikitable"})
        df =pd.read_html(str(bookertable))
        self.booker_df =pd.DataFrame(df[0])
        self.booker_df = self.booker_df.sort_values('Author')

    def xref_booker_pages(self):
        self.booker_titles_list = self.booker_df.Title.dropna().tolist()
        self.booker = self.wiki.page(self.booker_page)
        self.booker_wiki_links = list(self.booker.links.keys())
        self.booker_eligible_pages = list(set(self.booker_wiki_links) & set(self.booker_titles_list))
        # check page exists (throws out red links which are otherwise included by wiki.links.keys()):
        self.booker_eligible_pages = [pg for pg in self.booker_eligible_pages if self.wiki.page(pg).exists()]
        self.booker_eligible_pages.sort()


class WikiRead:
    """
    This is great work so far. We are able to pass a book's wiki page title, query it on wikipedia, \
    parse the article, and extract key info: author, title, and city, and country.
    """

    def __init__(self):
        """
        wiki_obj:
        """
        self.llm = Ollama(model="llama3")
        self.wiki = wikipediaapi.Wikipedia('MyProjectName (merlin@example.com)', 'en')

        return

    def pull_book(self, wiki_page):

        """

        wiki_obj: wikipediaapi object with page loaded, e.g. wiki =
            wikipediaapi.Wikipedia('MyProjectName (merlin@example.com)', 'en')
        wiki_page: valid wiki page title for wikipediaapi, ex: "Dune_(novel)"

        """
        prompt = "You are a bot that reads wikipedia articles about books and returns information about them. Your output must be in valid JSON. \
        Do not output anything other than the JSON.\
        First, find the book's title and add it to the JSON with the key 'title'.\
        Next, find the book's author and add it to the JSON with the key 'author'.\
        Next, find the city that the book is set in and add it to the JSON with the key 'city'. If the book is not set in a real city on earth or\
        you cannot determine what city it is set in, add 'NA' to the JSON with the key 'city'.\
        Next, find the country that the book is set in and add it to the JSON with the key 'country'. \
        If the book is not set in a real country on earth or you cannot determine what country it is set in, add 'NA' to the JSON\
        with the key 'country'. Finally, surround your JSON output with <result></result> tags. \
        Here is the text from the wikipedia article for this book: "

        end_prompt = "Based on the book's wikipedia article provided above, create a JSON describing this book. Your output must be in valid JSON. \
        Do not output anything other than the JSON.\
        First, find the book's title and add it to the JSON with the key 'title'.\
        Next, find the book's author and add it to the JSON with the key 'author'.\
        Next, find the city that the book is set in and add it to the JSON with the key 'city'. If the book is not set in a real city on earth or\
        you cannot determine what city it is set in, add 'NA' to the JSON with the key 'city'.\
        Next, find the country that the book is set in and add it to the JSON with the key 'country'. \
        If the book is not set in a real country on earth or you cannot determine what country it is set in, add 'NA' to the JSON\
        with the key 'country'. Finally, surround your JSON output with <result></result> tags."

        self.llm_response = self.llm(prompt + self.wiki.page(wiki_page).text + end_prompt)

        # parse the response
        json_string = self.llm_response.split('<result>')[1].split('</result>')[0]
        try:
            self.parsed_json = json.loads(json_string.strip())
        # in case the llm fails to return a valid json loadable string, we'll construct one and pass NAs
        except:
            json_string = '\n{\n"title": "NA",\n"author": "NA",\n"city": "NA",\n"country": "NA"\n}\n'
            self.parsed_json = json.loads(json_string.strip())

        self.parsed_json['page'] = wiki_page  # add in the page

        return self.parsed_json


