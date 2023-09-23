README 
DESCRIPTION:
This project is on Sentiment analysis on Roe v Wade using Social media data for CSE 6242 DVA (Fall 2022). A brief introduction is that when the US supreme court overturned the historic case on abortion rights it occupied the news channels and airwaves for weeks. Our project captures the varied responses and overall sentiment on this politically charged topic in a visual medium.
For the web scraping for Twitter tweets, we have used standard python libraries. For the scraping of Reddit posts, we have used praw package. PRAW, which stands for "Python Reddit API Wrapper", is a Python package that allows access to Reddit's API and is used for scraping the Reddit data.  
For the sentiment analysis, we have specifically used one python library: Transformers. Transformers provides an API that can download and train state-of-the-art pre-trained models. We wanted to run a pre-trained BERT model on our dataset, which is done through the transformers package. Using pre-trained models from transformers, we have reduced computing costs and have trained the model faster on large data.
For the visualization, we have used D3 v4 (https://d3js.org/d3.v4.js). It is an open-source Javascript library that is used for creating interactive visualizations using DOM structure.


INSTALLATION  
There are three parts to the project: data collection through web scraping, running sentiment analysis on the collected data, and running the D3 visualizations on the data.
Web Scraping:
For this part, we have scraped the data from Twitter and Reddit. For the environment setup, we are using the standard python libraries, so make sure to have a python installed in the local machine with a version of 3.7+. 
Twitter Data:
* We only used the standard python libraries and packages 
* To scrape the Twitter data, we have to request Twitter's Academic Research API access form.  For this, you have to fill out the following form: https://developer.twitter.com/en/portal/petition/academic/is-it-right-for-you
Reddit Data:
* To scrape Reddit, we have used the praw package. It can be installed in many ways.
* The most recommended way to download it is using pip. Pip usually comes is installed with the python library. But, if you don’t have pip, you can install it using this website: https://pip.pypa.io/en/stable/installation/
* After installing pip, just run the following command to install praw in your local machine: pip install praw
With this, our environment setup is done for web scraping
Sentiment Analysis:
For this part, we have used the Colab Notebook so that the environment setup is easy and no one has to download any python packages and libraries to run the code on their local machines. However, one requirement is that you need have access to google colab to run the file. You just need to have a Gmail account to use Google Colab
In the code folder, there is a file called sentiment_analysis.ipynb. 
* It is a Colab notebook, so it has to be run on Google Colab. Download this file from the code folder and load it on the google colab.
* We have used one main package in our code: transformers. We have already provided all the required import statements and methods, so you just have to only run the code cells and the environment setup will be complete. 
With this, the environment setup is done for the sentiment analysis.
D3 Visualizations:
For this, we have already provided the files in the code folder which has all the required d3 library files, so you don’t have to download any packages. To run the d3 locally, you will need to setup a local HTTP server in the root. 
* Execute the following commands based on the python version in the local machine.
* Python 2 — python -m SimpleHTTPServer 8000
* Python 3 — python -m http.server 8000
* After running the command, open a web browser at http://localhost:8000/.
With this, the environment setup for d3 visualization is done
EXECUTION:
Web Scraping:
After this, there are 2 main scripts to carry out data scrapping.
* tweet_scraper.py
Replace Twitter API-related strings, mostly bearer_token in the file at the top
Run the script as follows:

$ python3 tweet_scraper.py <hashtag>

* reddit_scraper.py
Replace Reddit API-related data in the Reddit object instantiation
Run the script as follows:

$ python3 reddit_scraper.py <subreddit>
After this, you will get the CSV files of the scrapped data, which will be used in the next part.
Sentiment Analysis:
We now want to run the sentiment analysis on the scraped data. After the Twitter and Reddit data has been web scrapped, you will have the CSV files with different hashtags, and we just want to run the sentiment analysis on those CSV files. 
Furthermore, if you don’t want to do the web scraping, we have provided a small dataset on both Twitter and Reddit posts in the folder called, sample dataset. That folder has four zip files: Reddit pulls, Twitter pulls, reddit_pulls_sentiment_output, and twitter_pulls_sentiment_outputs. Twitter pulls and Reddit pulls consist of scraped data on popular abortion hashtags. Furthermore, reddit_pulls_sentiment_output and twitter_pulls_sentiment_outputs are output files of the sentiment analysis of the same dataset. So, if you don’t want to run the sentiment analysis, so you directly go to the visualization part. 
Steps to run Sentiment Analysis:
   * Upload the sentiment_analysis.ipynb file on the google colab 
   * To run the sentiment analysis, we need a CSV file of Twitter tweets or Reddit posts. Make sure to have a text column for any Twitter CSV file and to have a title and body column for the Reddit CSV files. 
   * We have provided a sample code on how to run the sentiment analysis on the CSV files. You first have to upload the CSV files in the files tab in the colab and then uncomment the sample code and change the CSV file name in the sample code accordingly. 
   * After the code is run, it will download the output file, which contains two new columns: sentiment (one of the five labels) and sentiment score(range: -1 to 1). This output file will use in the visualization.
   * We have also provided a code for the sample data. To run that, first, upload the Twitter Pull and Reddit Pulls data in the colab and then uncomment the entire code and it generates all the output files.
D3 Visualizations:
There are three main scripts for the D3 visualizations:
   1. bubble.html: 
This creates a bubble plot for twitter and reddit data’s average sentiment score by hashtag or subreddit. Make sure that the input csv data file (“bubble.csv”) is in the same directory as the html file. You can just run the code and go to the local host web page and you will see the visualization there. 
   2. D3 bar chart:
This creates bar charts for both Democratic and Republican states depending on the average level of sentiment for these states. There is a pro-choice and a pro-life input for each of the two bar charts. Both inputs are loaded into the html file as generated by the ipynb file in the D3 Visualizations folder.You can just run the code and go to the local host web page and you will see the visualization there. 
   3. D3 chloropleth:
This creates chloropleth maps for the pro-life tweets and pro-choice tweet sentiments for all US states. There is a separate chloropleth map for pro-choice tweets and pro-life tweets. Both inputs are loaded into files/DataInput as generated by the ipynb file in the D3 Visualizations folder. You can just run the code and go to the local host web page and you will see the visualization there.