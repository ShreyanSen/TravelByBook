
import wikipediaapi
from langchain_community.llms import Ollama
import pandas as pd # library for data analysis
import requests # library to handle requests
from bs4 import BeautifulSoup # library to parse HTML documents

import json


class WikiFetch:

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

    def __init__(self, wiki_obj):
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


