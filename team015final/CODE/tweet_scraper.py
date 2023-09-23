#! /bin/env python3
import json
import requests
from csv import DictWriter, writer
import math

# set bearer token here
bearer_token = ""

# base url
search_url = "https://api.twitter.com/2/tweets/search/recent"

# oof! done.
def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r

# actual data gathering step
def connect_to_endpoint(url, params):
    response = requests.get(url, auth=bearer_oauth, params=params)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    
    print("Collected {} tweets!".format(max_results))
    return response.json()


def main():
    file = open(hashtag + ".csv", "a")
    field_names = ["timestamp", "location", "text"]
    headerWriter = writer(file)
    headerWriter.writerow(field_names)

    dictwriter_object = DictWriter(file, fieldnames=field_names)
    
    # boring!!
    for i in range(iterations):
        response = connect_to_endpoint(search_url, query_params)
        
        for i, tweet in enumerate(response["data"]):
            total_users = len(response["includes"]["users"])
            user = response["includes"]["users"][min(i, total_users-1)]

            entry = {
                "text": tweet["text"],
                "location": user["location"] if "location" in user else "", # edge case
                "timestamp": tweet["created_at"]
            }
            dictwriter_object.writerow(entry)

        query_params["next_token"] = response["meta"]["next_token"]
        # API pagination adjustment
    file.close()

        
if __name__ == "__main__":

    # CHANGE
    hashtag = "#prolife"

    # CHANGE
    target_tweets = 100

    # DO NOT CHANGE
    max_results = 100
    iterations = math.ceil(target_tweets/max_results) 

    query_params = {
                       "query": hashtag + " -is:retweet -is:quote -is:reply",
                       "expansions": "geo.place_id,referenced_tweets.id,author_id",
                       # set appropriate date based on the data required
                       # CHANGE
                       "start_time": "2022-11-25T00:00:00.000Z",
                       # CHANGE
                       "end_time": "2022-11-27T00:00:00.000Z",
                       "tweet.fields": "author_id,created_at,geo,text",
                       "user.fields": "location,name",
                       "place.fields": "full_name,contained_within"
                    }
    main()
