import json
import os
import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
import time
from functools import partial

class GeoBookJSONtoCSV:

    def __init__(self, data_dir = 'data/json/', db_dir = 'data/csv/'):
        self.data_dir = data_dir
        self.db_dir = db_dir
        self.data_files = os.listdir(self.data_dir) # assumes pure directory of just processed book jsons

    def update_book_df(self):

        self.book_df = pd.read_csv(self.db_dir+'uncorrected_book_df.csv')
        self.correct_book_df_errors()
        self.write_book_df()

    def run_all(self):

        self.get_book_df()
        self.geolocate_books()
        self.correct_book_df_errors()
        # self.group_by_address() # not needed currently
        self.write_book_df()
        
    def get_book_df(self):
        books_list = []
        for data in self.data_files:
            with open(self.data_dir + data, 'r') as f:
                book = json.load(f)
                books_list.append(book)
        df = pd.DataFrame(books_list)
        df.replace('NA', np.nan, inplace=True)
        df.replace('', np.nan, inplace=True)
        df.replace('None', np.nan, inplace=True)


        # make sure we can index on unique 'page' column
        try:
            len(df) == df.page.nunique()
        except:
            print('page column does not uniquely index rows! dropping rows')
            df = df.drop_duplicates('page')
            pass
        # generate city_country column

        df_city_country = df.loc[~(df.city.isna()) & ~(df.country.isna())] # city and country listed

        df_city_country = df_city_country.assign(city_country=np.nan) # create empty col
        df_city_country.loc[:,'city_country'] = df_city_country.loc[:,'city'] +', '+ df_city_country.loc[:,'country']
        #df_city_country['city_country'] = df_city_country['city'] +', '+ df_city_country['country']

        df_country = df.loc[(df.city.isna()) & ~(df.country.isna())] # country but no city listed
        df_country = df_country.assign(city_country=np.nan)
        df_city = df.loc[~(df.city.isna()) & (df.country.isna())] # city but no country listed
        df_city = df_city.assign(city_country=np.nan)

        self.df_list = [df_city_country, df_city, df_country]
        self.book_df = pd.concat(self.df_list)

        self.df_city_country = df_city_country
        self.df_city = df_city
        self.df_country = df_country

    def geolocate_books(self):
        """
        IMPORTANT: the geolocation call actually gives a location for None or np.nan lol. So, need to avoid passing those entries in!
        Replacing with nan where missing, we want to acquire the city_location, country_location, and city_country_location. 
        """
        geolocator = Nominatim(user_agent="geobooks", timeout=10)
        geocode = partial(geolocator.geocode, exactly_one=True, language="en", addressdetails=True) # defines language as english

        print(time.ctime())
        self.df_city_country = self.df_city_country.assign(city_location=None, country_location=None, city_country_location=None)
        print(time.ctime())
        self.df_city_country['city_location'] = self.df_city_country.apply(lambda x: geocode(x.city), axis=1)
        print(time.ctime())
        self.df_city_country['country_location'] = self.df_city_country.apply(lambda x: geocode(x.country), axis=1)
        print(time.ctime())
        self.df_city_country['city_country_location'] = self.df_city_country.apply(lambda x: geocode(x.city_country), axis=1)
        print(time.ctime())
        self.df_city = self.df_city.assign(city_location=None, country_location=None, city_country_location=None)
        print(time.ctime())
        self.df_city['city_location'] = self.df_city.apply(lambda x: geocode(x.city), axis=1)
        print(time.ctime())
        self.df_country = self.df_country.assign(city_location=None, country_location=None, city_country_location=None)
        print(time.ctime())
        self.df_country['country_location'] = self.df_country.apply(lambda x: geocode(x.country), axis=1)

        print(time.ctime())
        self.df_list = [self.df_city_country, self.df_city, self.df_country]
        self.book_df = pd.concat(self.df_list)

        self.book_df = self.select_best_location(self.book_df)

        # log uncorrected book df; in other steps we load and update this 
        self.book_df.to_csv(self.db_dir+'uncorrected_book_df.csv') 
        
        
        return 

    def correct_book_df_errors(self):
        """
        In this function we manually correct errors introduced through our pipeline which have been noticed and flagged.
        Just a way of doing this in code and tracking it--not ideal but it will work as a placeholder.
        Note: we have some duplicates because the same book was referenced under different page names on wikipedia
        """
        # drops a near duplicate of Dark Cloud
        self.book_df = self.book_df.loc[~((self.book_df.title=='Dark Cloud') & (self.book_df.page=='Aztec (book)'))] 
        # drops a near duplicate of Footsteps with an incorrect address
        self.book_df = self.book_df.loc[~((self.book_df.title=='Footsteps') & (self.book_df.address=='Batavia, Solano County, California, United States'))]
        # drops a near duplicate of The Known World with less accurate address
        self.book_df = self.book_df.loc[~((self.book_df.title=='The Known World') & (self.book_df.address=='United States'))]
        # drops a near duplicate of If Nobody Speaks of Remarkable Things with a less accurate address
        self.book_df = self.book_df.loc[~((self.book_df.title=='If Nobody Speaks of Remarkable Things') & (self.book_df.address=='United Kingdom'))]

        # drops a near duplicate of Marthandavarma with less accurate address
        self.book_df = self.book_df.loc[~((self.book_df.title=='Marthandavarma') & (self.book_df.city=='Travancore (now Trivandrum)'))]
        # drops a near duplicate of Marthandavarma with less accurate address
        self.book_df = self.book_df.loc[~((self.book_df.title=='Marthandavarma') & (self.book_df.city=='Travancore'))]

        self.nan_author_pages_authors_dict = {'The Sicilian':'Mario Puzo',
        'El Buscón':' Francisco de Quevedo',
        'A Fine Balance':'Rohinton Mistry',
        'Jasper Jones':'Craig Silvey',
        'Clear Light of Day':'Anita Desai',
        'Prague (novel)':'Arthur Phillips',
        'History of Wolves':'Emily Fridlund',
        'Fortunata y Jacinta':'Benito Pérez Galdós' ,
        'The Tale of Genji':'Murasaki Shikibu',
        'Under the Eagle': 'Simon Scarrow',
        'Lazarillo de Tormes':'Anonymous',
        'Aztec (novel)':'Gary Jennings',
        'The Coffee Trader':'David Liss',
        'Captain Alatriste':'Arturo Pérez-Reverte',
        'Ducks, Newburyport':'Lucy Ellmann',
        'Woodstock (novel)':'Sir Walter Scott',
        'The Leopard':'Giuseppe Tomasi di Lampedusa',
        "Barnaby Rudge: A Tale of the Riots of 'Eighty":'Charles Dickens',
        'The Story of the Stone':'Cao Xueqin',
        'The Deer and the Cauldron':'Jin Yong',
        'The Orenda':'Joseph Boyden',
        'The Black Coat':'Neamat Imam'}

        for page in self.nan_author_pages_authors_dict:
            self.book_df.loc[self.book_df.page==page, 'author'] = self.nan_author_pages_authors_dict[page]
        print(self.book_df.loc[self.book_df.page=='The Black Coat'])
    def group_by_address(self):
        self.book_df_group = self.book_df.groupby(['geocoded_address','lat','lon'])['title'].apply(list).reset_index()
        self.book_df_group['title_str'] =self.book_df_group.apply(lambda x: [str(t) for t in x.title], axis=1) # sometimes titles are not strings
        self.book_df_group['titles'] = self.book_df_group.apply(lambda x:"<br>".join(x.title_str), axis=1)
        self.book_df_group = self.book_df_group[['geocoded_address','lat','lon','titles']]
        self.book_df_group = self.book_df_group.loc[~(self.book_df_group.geocoded_address=='')] # drops instances where no legitimate address found
        
        return

    def write_book_df(self):
        self.book_df.to_csv(self.db_dir+'book_db.csv')
        # self.book_df_group.to_csv(self.db_dir+'books_per_coord_db.csv') # no longer needed

    def select_best_location(self, df):
        df = df.assign(best_location=None)
        df.loc[~df.city_country_location.isna(),'best_location'] =  df.loc[~df.city_country_location.isna(),'city_country_location']
        df.loc[(df.city_country_location.isna()) & (~df.city_location.isna()),'best_location'] = df.loc[(df.city_country_location.isna()) & (~df.city_location.isna()),'city_location']
        df.loc[(df.city_country_location.isna()) & (df.city_location.isna()) & (~df.country_location.isna()),'best_location'] = df.loc[(df.city_country_location.isna()) & (df.city_location.isna())  & (~df.country_location.isna()),'country_location']
    
    
        df = df.assign(lat=None, lon=None, address=None, geocoded_address=None, geocoded_country=None)
        #location.latitude, location.longitude
        df.loc[~df.best_location.isna(), 'lat'] = df.loc[~df.best_location.isna()].apply(lambda x: x.best_location.latitude, axis=1)
        df.loc[~df.best_location.isna(), 'lon'] = df.loc[~df.best_location.isna()].apply(lambda x: x.best_location.longitude, axis=1)
        df.loc[~df.best_location.isna(), 'address'] = df.loc[~df.best_location.isna()].apply(lambda x: x.best_location.address, axis=1)                                                                        
        df.loc[~df.best_location.isna(), 'geocoded_address'] = df.loc[~df.best_location.isna()].apply(lambda x: self.get_geocoded_address(x.best_location), axis=1)                                                                        
        df.loc[~df.best_location.isna(), 'geocoded_country'] = df.loc[~df.best_location.isna()].apply(lambda x: x.best_location.raw['address'].get('country'), axis=1)                                                                        

        return df  

    @staticmethod
    def get_geocoded_address(location):
        """
        function takes in a geolocator.geocode returned object with address_details=True
        returns an address that just has any subset of City, State, Province, or Country values 
        The .get returns None if the city key doesn't exist, and then the filter None drops it if None
        """
        geocoded_address = ', '.join(filter(None, [location.raw['address'].get('city'),location.raw['address'].get('province'), location.raw['address'].get('state'), location.raw['address'].get('country')]))
        return geocoded_address
        
    @staticmethod
    def jitter_duplicate_coords(df):
        df2 = df.copy(deep=True)
        df2['dupe_idx'] = df2.groupby(['lon']).cumcount() # 0 if no duplicates, if not indexes the duplicate
        #df['lon_og'] = df['lon']
        df2['lon'] = df2['lon'] + 0.01*df2['dupe_idx']
        return df2


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