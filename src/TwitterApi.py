# Queries from the TwitterApi a bunch of tweet data based on a query and timeframe
# So the main function needs to constantly call the function every (x) minutes
# can implement livestreaming later, but will probably not be needed

# Output:
# Dataframe: index, id, text,
# "entities" is a dict of hashtags, urls, cashtags, and annotations
# "context_annotations" is confusing. It tries to find subjects in the text and classify them
#       like "stock" "hospitality" "travel" "personal finance". Sometimes non-domain things as well
#       also it can sometimes pick out specific company names

# In this version I got all the necessary object data from Twitter's end
# I just need to parse through them and make them into distinct columns
import requests
import setup
import pandas as pd
import datetime

def main():
    print(searchTwitter(query="Walgreens", timeframe=30))


# format: searchTwitter(query=(str), timeframe=(int))
# if you want an exact query match, you need to use single quotes "'exact keyword'"
def searchTwitter(**kwargs):
    # checks for the necessary function arguments
    try:
        kwargs["query"]
    except KeyError:
        raise Exception("searchTwitter() Must have a query. Usage: searchTwitter(query = str)")
    try:
        kwargs["timeframe"]
    except KeyError:
        raise Exception("searchTwitter() Must have a time. Usage searchTwitter(time = int)")

    # authorization for the twitterapi endpoint
    headers = {
        "Authorization": "Bearer " + setup.twitterBearerToken
    }
    # these are the parameters for search/stream
    fixed_query_operators = " -is:retweet lang:en"
    payload = {
        "query": kwargs["query"] + fixed_query_operators,
        "start_time": getOldTime(kwargs["timeframe"]),
        "tweet.fields": "created_at,attachments,"+
                        "context_annotations,entities,possibly_sensitive,referenced_tweets",
        # "user.fields":
        # "poll.fields":
        # "place.fields":
        # "media.fields":
        "expansions": "author_id,referenced_tweets.id,in_reply_to_user_id,attachments.media_keys,"+
                        "entities.mentions.username,referenced_tweets.id.author_id"
    }
    # makes sure the query is less than the twitter query limit
    if len(kwargs["query"]+fixed_query_operators) > 512:
        raise Exception("Total query is too long (>512 characters)")

    # retrieves the first Twitter JSON object response and creates a dataframe from it
    r = requests.get("https://api.twitter.com/2/tweets/search/recent", params=payload, headers=headers)
    print(r.json())
    df = pd.DataFrame(columns=["id", "text", "created_at", "attachments", "context_annotations",
                               "entities", "possibly_sensitive", "referenced_tweets",
                               "author_id", "referenced_tweets.id", "in_reply_to_user_id", "attachments.media_keys",
                               "entities.mentions.username", "referenced_tweets.id.author_id"])
    df = df.append(pd.DataFrame(r.json()["data"]))

    # Iterates to add all response pages to the dataframe
    isLastPage = False
    while not isLastPage:
        try:
            payload["next_token"] = r.json()["meta"]["next_token"]
            r = requests.get("https://api.twitter.com/2/tweets/search/recent", params=payload, headers=headers)
            df = df.append(pd.DataFrame(r.json()["data"]), ignore_index=True)
        except KeyError:
            isLastPage = True

    # formats and returns the dataframe
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    return df

# Gets the UTC time var minutes ago in ISO 8601/RFC 3339 format, returns a str
def getOldTime(minutes):
    oldtime = datetime.datetime.utcnow() - datetime.timedelta(minutes=minutes)
    RFC_format = str(oldtime).replace(" ", "T")[:19]
    return RFC_format + "Z"

main()
