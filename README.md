# Travel By Book

This repo includes data, an AI-powered data generation pipeline, and streamlit front end logic for the Travel By Book app (https://travelbybook.streamlit.app/). 

## Description of Travel By Book

The idea for Travel By Book is based on a wonderful experience I had reading Tan Twan Eng's The House of Doors while I was visiting Penang, Malaysia. The author is a Penang local and the book is set in Penang, and its rich descriptions of Penang's history and architecture helped me see what was around me. Sitting in a local cafe reading a book about the place you're visiting--what a relaxing way to travel.

So I thought, wouldn't it be nice to be able to do this wherever I go? Of course, the best way is to do exactly what I did. Go to a local bookstore, make friends with the shopkeeper, and eagerly accept their recommendation on what to read. But, what's your second best option? I thought it would be nice to have a database of books that have some connection to places. But how would I get the right books, and how would I assess where they are set?

One way is to get some verified lists of good books (for example, prize winners), and to check where each one is set. That's a lot of checking though. I decided to take a very long detour. Why not have AI read the descriptions for me and tell me where the books are set? So that's exactly what I did. I wrote code that systematically prepares wikipedia pages of eligible books based on scraped content from lists of such books, queries the pages, sends the queried text to a local instance of llama3, and assesses where the books are set (if set anywhere real--Dune, for example, shouldn't make our list). 

Then, all that was left was to share the database out. Please enjoy!

## Description of Data Pipeline

The pipeline has a few key stages:

* Generating a list of wikipedia pages for selected books
* Querying each wikipedia page, passing the contents to an LLM, and classifying the books
* Aggregating results into a csv and making quality control adjustments


## How to Run Data Pipeline

## Notes on Experience Building Travel by Book