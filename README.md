# Travel By Book

This repo includes data, an AI-powered data generation pipeline, and streamlit front end logic for the Travel By Book app (https://travelbybook.streamlit.app/). 

## Description of Travel By Book

Travel By Book was born while traveling. I was sitting in a cafe in Penang, reading Tan Twan Eng's The House of Doors. The book is set in Penang and tells the story of my favorite author's own time in Penang. Tan is a Penang local, and his book's rich descriptions of Penang's history, society, and architecture opened my eyes to what was around me.

Wouldn't it be nice, I thought, to be able to do this anywhere? Of course, the best way is to do exactly what I did. Go to a local bookstore, make friends with the shopkeeper, and ask for the local treatment. But, what's your second best option? I thought it would be nice to have a database of books that have some connection to places. But how would I get the right books, and how would I assess where they are set?

One way is to get some verified lists of good books (for example, prize winners), and to check where each one is set. That's a lot of checking. Like any decent coder I decided to take a very long shortcut. Why not have AI read the descriptions for me and tell me where the books are set? So that's what I did. I wrote code that scrapes lists of books for their wikipedia pages, queries the pages, sends the queried text to a local instance of llama3, and assesses where the books are set (if set anywhere real--Dune, for example, shouldn't make our list). 

I found the whole process worth sharing. Please enjoy!

## Description of Data Pipeline

The pipeline has a few key stages:

* Generating a list of wikipedia pages for selected books
* Querying each wikipedia page, passing the contents to an LLM, and classifying the books
* Aggregating results into a csv and making quality control adjustments

Where do lists of selected books come from? I started with the list of all Booker winners and nominees (https://en.wikipedia.org/wiki/List_of_winners_and_nominated_authors_of_the_Booker_Prize), as well as a list of historical novels by country (https://en.wikipedia.org/wiki/List_of_historical_novels). The booker list was fairly straightforward to scrape, but the list of historical novels was not in an easily queryable format. I scraped the set of all links on the page, queried the contents for each linked page, passed them to my local instance of llama 3 (run through ollama), and asked it to judge whether the page was a book or not. This was a time consuming process because there were a large number of false positive reports of books (e.g. llama3 identified the wiki page of an author as the wiki page of a book). To address this, I passed in queries of various subsets of each wikipedia page, making a strict filter condition whereby each page had to be classified as a book multiple times. This no doubt led to a higher false negative rate where real books in the original set of links did not make it to our list of verified books, but since our database of books makes no claim as to completeness or representativeness, a higher false negative rate for very low false positive rate was acceptable. This "run high volume, filter results tightly" approach wouldn't work for all data products. Important classes for these steps can be found in src/wiki.py. 

Once we have orderly list of wikipedia pages that are valid books, we use more web scraping tools to query the texts and pass them to our local instance of llama 3, run through ollama. I ask llama3 to determine the title, author, and location (city and country) of each book. This part is also quite tricky, both to get correct answers and even to return results in machine-readable format. Significant experimentation with prompt engineering was required. We also lose some books here to wrong answers from the llm. For each book we process, the llm writes a json with the information required. Important classes for these steps can be found in src/wiki.py.

From here, it's just a matter of reading the json files as a csv, taking the location data, and using a geolocator to find lat, lon, and addresses. Of course, not every book's llm-generated location text strings map to a real address or lat lon. We lose more books here. But from the books we are able to geolocate, we then use the geolocated address to build standardized text descriptions of locations and lat lons to pass to mapping software. This all goes into dataframe written as a csv. Important classes for these steps can be found in src/db.py.

Because this pipeline involves so much space for llm error, we also introduce a place for manual but machine-traceable quality control. The GeoBookJSONtoCSV class in src/db.py has a correct_book_df_errors() function where we make changes to our final dataframe / csv. The pipeline can also call this function on its own, so a new additional change can be integrated by running this part of the pipeline in a standalone way (and of course it's idempotent)

Finally we use streamlit to create an app that users can interact with. This involves some a tight coupling of geospatial processing and design choices. For example, it took me some thought to decide how to deal with books located at the same place. Since my mapping method would cover one point with another, I initially tried lightly offsetting duplicate geometries, before ultimately deciding to group by lat lon and display sets of books by location. Important classes for these steps can be found in src/wiki.py. 


## How to Run Data Pipeline

The entry point for the pipeline is the gen_book_db.py script in the root directory. This script has 5 flags corresponding to the various modes it can be run it. Here are examples of how to use them.

`python gen_book_db.py -pagelist-source -$yourpagelistsource`
generates a pagelist from the source indicated (for example the booker wiki page) and by default queries the pages in the list and creates those books' llm-classified json files. by setting the additional flag... 
`python gen_book_db.py -pagelist-source -$yourpagelistsource -gen-json-data False`
...you can create a pagelist file without running the next phase of the pipeline and creating book json data. This is because -gen-json-data is by default set to True.

`python gen_book_db.py -pagelist-file -$yourpagelistfile`
does the same thing, except instead of generating a pagelist from a viable source (like 'booker') it loads an already generated pagelist at the location you pass in, and then runs the next phase of the pipeline to create book json data. 

`python gen_book_db.py -gen-csv-db True`
generates the csv database from book json data (which is expected at data/json/), geolocates the data points, and does quality control updates to the database. 

`python gen_book_db.py -update-csv-db True`
takes the already existing csv database (meaning -gen-csv-db True must have been run once in the past) and only performs quality control updates. It does this by loading a cached version of the file with no updates and then passing it through the update function. This means the process is idempotent--you can run it, change your update function, and run it again, and you'll get the correct output. 


Data at each stage is contained within a pre-specified location within the /data directory. This can be changed, but the pipeline expects data to be in these locations at various points in the pipeline, so deviate from this file structure at your own risk. Specifically, pagelists are under /data/pagelists, llm-classified book data are stored under data/json, and the final csv fed to the streamlit app is stored under data/csv.


## Notes on Experience Building Travel by Book

I wanted to make this process as automated as I could, but I found there were major tradeoffs involved. Primarily, these occurred because each llm step had a lot of room for error. The llm was not above misclassifying books as non-books or non-books as books, or misunderstanding where a book is set. As a result, many eligible books were probably excluded, and of those that are included, mistakes probably exist in the database.

Because of these mistakes, I added a function that lets me make adjustments to the final db. This is basically the same as overwriting the db "by hand" but in a way that's tracked and reproducible. 

LLMs are fantastic and there is no way I'd sort through 600 wikipedia pages myself creating this database. But they are not perfect and they do not replace the fine touch of human labor, nor do they account for taste. Because of this, I'll keep this database open to suggestions for new books (or book lists) to include, and open to crowdsourced feedback on errors in the database I can correct manually. Over time, we should be able to catch the issues and make improvements. The machine rough cut a nice chunk of stone for us, and now comes gentle polishing. 