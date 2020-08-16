# This version doesn't have the time function implemented
# and it only retrieves queries from Andrew Yang
#
# This is just a recent search function right now
# So the main function needs to constantly call the function every (x) minutes
# can implement livestreaming later, but will probably not be needed

import requests
import setup
import pandas as pd


def main():
    print(searchTwitter(query="it", time="0"))


# format: searchTwitter(query=(), time=(),  )
# query is a string, time is int minutes,
# if you want an exact query match, you need to use single quotes "'exact keyword'"
def searchTwitter(**kwargs):
    # checks for the necessary function arguments
    try:
        kwargs["query"]
    except KeyError:
        raise Exception("searchTwitter() Must have a query. Usage: searchTwitter(query = str)")

    try:
        kwargs["time"]
    except KeyError:
        raise Exception("searchTwitter() Must have a time. Usage searchTwitter(time = int)")

    # authorization for the twitterapi endpoint
    headers = {
        "Authorization": "Bearer " + setup.twitterBearerToken
    }

    # these are the parameters for search/stream
    fixed_query_operator = " from:AndrewYang -is:retweet lang:en"
    payload = {
        "query": kwargs["query"] + fixed_query_operator,
        "start_time": "2020-08-15T00:00:01Z",
        "tweet.fields": "created_at,attachments"
    }
    # makes sure the query is less than the twitter query limit
    if len(kwargs["query"]+fixed_query_operator) > 512:
        raise Exception("Total query is too long (>512 characters)")

    # retrieves the first Twitter JSON object response and creates a dataframe from it
    r = requests.get("https://api.twitter.com/2/tweets/search/recent", params=payload, headers=headers)
    print(r.json())
    df = pd.DataFrame(r.json()["data"])

    # Iterates to add all response pages to the dataframe
    isLastPage = False
    while not isLastPage:
        try:
            next_token = r.json()["meta"]["next_token"]
            payload["next_token"] = next_token
            r = requests.get("https://api.twitter.com/2/tweets/search/recent", params=payload, headers=headers)
            df = df.append(pd.DataFrame(r.json()["data"]), ignore_index=True)
        except KeyError:
            isLastPage = True

    # returns the dataframe
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    return df


main()
