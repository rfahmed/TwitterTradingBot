# Queries from the TwitterApi a bunch of tweet data based on a query and timeframe
# So the main function needs to constantly call the function every (x) minutes
# can implement livestreaming later, but will probably not be needed

# Output:
# Dataframe: index, id, text,
# "entities" is a dict of hashtags, urls, cashtags, and annotations
# "context_annotations" is confusing. It tries to find subjects in the text and classify them
#       like "stock" "hospitality" "travel" "personal finance". Sometimes non-domain things as well
#       also it can sometimes pick out specific company names
# If there are no tweets, it will output a string "No New Tweets"
# All the columns with a . in them are going to be lists

# I finished the parse function in this version
# I just need to check this against all domain paths
# Check if indexes line up
# Attach the returned dfs to the original df
# Do the same for the expansion tweet df
# Clean up the comments

import requests
import setup
import pandas as pd
import datetime
import numpy as np

def main():
    searchTwitter(query="Walgreens", timeframe=35)
    #print(searchTwitter(query="Walgreens", timeframe=10))


# format: searchTwitter(query=(str), timeframe=(int))
# if you want an exact query match, you need to use single quotes "'exact keyword'"
def searchTwitter(**kwargs):
    # checks for a query and timeframe argument
    try:
        kwargs["query"]
    except KeyError:
        raise Exception("searchTwitter() Must have a query. Usage: searchTwitter(query = str)")
    try:
        kwargs["timeframe"]
    except KeyError:
        raise Exception("searchTwitter() Must have a time. Usage searchTwitter(time = int)")


    # Authorization for the twitterapi endpoint and the parameters
    headers = {
        "Authorization": "Bearer " + setup.twitterBearerToken
    }
    # these are the parameters for search/stream
    fixed_query_operators = " -is:retweet lang:en"
    payload = {
        "query": kwargs["query"] + fixed_query_operators,
        "start_time": getOldTime(kwargs["timeframe"]),
        "tweet.fields": "created_at,context_annotations,entities,possibly_sensitive,referenced_tweets",
        "expansions": "referenced_tweets.id"
    }
    # makes sure the query is less than the twitter query limit
    if len(kwargs["query"]+fixed_query_operators) > 512:
        raise Exception("Total query is too long (>512 characters)")


    # retrieves the first Twitter JSON object response and creates a dataframe from it
    r = requests.get("https://api.twitter.com/2/tweets/search/recent", params=payload, headers=headers)
    df = pd.DataFrame(columns=["id", "text", "created_at", "context_annotations",
                               "entities", "possibly_sensitive", "referenced_tweets"])
    # Prevents no new tweets returned from breaking the code
    try:
        df = df.append(pd.DataFrame(r.json()["data"]))
    except KeyError:
        return "No New Tweets"
    # This creates another dataframe based on the referenced tweet expansion object ("replying to, quoting, etc.")
    expansiondf = pd.DataFrame(columns=["id", "text", "created_at", "context_annotations",
                               "entities", "possibly_sensitive", "referenced_tweets"])
    # Prevents no reference tweets returned from breaking the code
    try:
        expansiondf = expansiondf.append(pd.DataFrame(r.json()["includes"]["tweets"]))
    except KeyError:
        pass


    # Twitter data comes in pages
    # This pages through and adds all addition tweets and expansion objects to their respective dfs
    isLastPage = False
    while not isLastPage:
        try:
            payload["next_token"] = r.json()["meta"]["next_token"]
            r = requests.get("https://api.twitter.com/2/tweets/search/recent", params=payload, headers=headers)
            df = df.append(pd.DataFrame(r.json()["data"]), ignore_index=True)
            # Prevents no reference tweets returned on this page from breaking the code
            try:
                expansiondf = expansiondf.append(pd.DataFrame(r.json()["includes"]["tweets"]), ignore_index=True)
            except KeyError:
                pass
        except KeyError:
            isLastPage = True

    # Changes the header of the referenced_tweet table so you know which data is from the referenced_tweet (ref_twt)
    expansiondf = expansiondf.rename(columns={"id": "ref_twt_id", "text": "ref_twt_text",
                                              "created_at": "ref_twt_created_at",
                                              "context_annotations": "ref_twt_context_annotations",
                                              "entities": "ref_twt_entities",
                                              "possibly_sensitive": "ref_twt_possibly_sensitive",
                                              "referenced_tweets": "ref_twt_referenced_tweets"})

    # Parses the referenced_tweet object in df into two columns: ref_type and ref_twt_id
    # That way we can attach df and expansiondf side by side and use the ref_twt_id to line them up
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('max_colwidth', 400)
    print(df)
    print(parseObjectColumn(df, "entities", "entities.urls.url", "entities.hashtags.tag"))

    # Parses the object columns into distinct columns
    # i.e. takes a dict of entities and seperates them into columns of "hashtags", "cashtags", "mentions", etc.
        #parseObjectColumn(df["entities"], "entities.hashtag", "test2", "test3")

    # formats and returns the dataframe
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)

    #df = df[["id", "text", "created_at", "entities"]]

    #print(expansiondf)
    #print(df)
    return df

# This takes a column of objects and separates them out into distinct columns
# It takes in the column/df and the dict routes of the data you want to extract
# And will output a dataframe of your desire data in their own columns
def parseObjectColumn(df, header, *args):

    # Twitter's context_annotations, entities, and referenced tweets are different object types
    # Some are lists of dict of dicts or dicts of lists of dicts etc.
    # This if, elif, else statement just separates what type of column we got
    # Data will be a 2Xlen(args) list that will have all the data we will eventually turn into a dataframe
    data = []
    if header == "context_annotations" or header == "ref_twt_context_annotations":
        # Since we have to parse through each element for each data we want we need to do this process
        # X number of times where X is the number of unique data types we want
        for i in range(len(args)):
            # Imagine the templist as a single column. Data[] is a list of lists or a collection of columns
            templist = []
            # This goes down each element and looks at the object within it
            for row in df.loc[:, header]:
                # An object may have many pieces of data of the same data type so our element will be a list
                element = []
                # If there's nothing in the element then just move on
                if not pd.isnull(row).all():
                    # This takes in the args and splits it into a list of the key names
                    address = args[i].split(".")
                    # Twitter's context_annotations object is a list of dicts. So this for loop makes
                    # Sure we parse through every dict
                    for j in range(len(row)):
                        # Each dict may have a domain or entity key. So we need a try/except statment
                        # Otherwise the code will have a KeyError if the Domain/Entity key DNE
                        try:
                            # Adds to element[] whatever data is at that listdictdict address
                            element.append(row[j][address[1]][address[2]])
                        except KeyError:
                            pass
                    # Adds element to the templist (aka a column)
                    templist.append(element)
                else:
                    templist.append(np.nan)
            # Adds the templist (column) to the data list
            data.append(templist)

    elif header == "entities" or header == "ref_twt_entities":
        # Look at the first if statement for the same idea. Only difference is that
        # Twitter's entities object is a dict of a list of dicts. So things are slightly rearrange but you
        # Should be able to infer
        for i in range(len(args)):
            templist = []
            for row in df.loc[:, header]:
                element = []
                if not pd.isnull(row):
                    address = args[i].split(".")
                    try:
                        for j in range(len(row[address[1]])):
                            element.append(row[address[1]][j][address[2]])
                        templist.append(element)
                    except KeyError:
                        templist.append(np.nan)
                else:
                    templist.append(np.nan)
            data.append(templist)
    elif header == "referenced_tweets" or header == "ref_twt_referenced_tweets":
        # Look at the first if statement for the same idea
        # Only difference is that we don't have to check for multiple dicts
        # When we're in the element so the code is simplier
        for i in range(len(args)):
            templist = []
            for row in df.loc[:, header]:
                element = []
                if not pd.isnull(row):
                    for j in range(len(row)):
                        element.append(row[j][args[i]])
                    templist.append(element)
                else:
                    templist.append(np.nan)
            data.append(templist)
    else:
        raise Exception("Header not recognized")

    # This takes the 2xlen(args) list and enters into a dataframe such that the column headers is args
    # This new dataframe is what will be slapped out to the side of the original df
    # This for args loop just to create new titles based on the old column name + the title of the individual data
    # Only really needed for referenced_tweets so the else section is normal
    if header == "referenced_tweets" or header == "ref_twt_referenced_tweets":
        newheader = []
        for arg in args:
            newheader.append(header+"."+arg)
        newdf = pd.DataFrame(index=range(0, len(df.loc[:, header])), columns=newheader)
        for i in range(len(df.loc[:, header])):
            for j in range(len(args)):
                newdf.loc[i, newheader[j]] = data[j][i]
    else:
        newdf = pd.DataFrame(index=range(0, len(df.loc[:, header])), columns=args)
        for i in range(len(df.loc[:, header])):
            for j in range(len(args)):
                newdf.loc[i, args[j]] = data[j][i]

    return newdf


# Gets the UTC time var minutes ago in ISO 8601/RFC 3339 format, returns a str
def getOldTime(minutes):
    oldtime = datetime.datetime.utcnow() - datetime.timedelta(minutes=minutes)
    RFC_format = str(oldtime).replace(" ", "T")[:19]
    return RFC_format + "Z"

main()
