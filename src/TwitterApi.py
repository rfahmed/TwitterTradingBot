# Queries from the TwitterApi a bunch of tweet data based on a query and timeframe
# So the main function needs to constantly call the function every (x) minutes
# can implement livestreaming later, but will probably not be needed

# Output DataFrame Column Headers:
# id: unique id given to every tweet
# text: text of tweet
# Created_at: date created
# context_annotations.domain.name: annotations of the content of the tweet
# context_annotations.domain.descriptions describes what each domain.name means. i.e. brand vertical = an industry
# context_annotations.entity.name: tries to identify nouns. (common noun or proper nouns). like companies
# context_annotations.entity.description: describes the industry/role/position of the entity
# entities.annotations.probability: probability of the following entity being true
# entities.annotations.type: entity type (person, place, organization, etc.)
# entities.annotations.normalized_text: if it exists, its the specific above (i.e. company -> Google)
# entities.urls.url: urls in the tweet
# entities.hashtags.tag: hashtags in the tweet
# entities.mentions.username: mentions in the tweet
# entities.cashtags.tag: cashtags mentioned in the tweet
# possibly_sensitive: nsfw or not
# referenced_tweets.type: if the tweet references another, how (retweet with comment, reply to, quote, etc.)
# referenced_tweets.id: twitter id of the referenced tweet
# There will be more columns with a prefix "ref_twt_" which signfies all the tweet data as above, except for the
# referenced tweet, if there is one.
#
# If there are no tweets within the timeframe, it will output a string "No New Tweets"
# Column headers with a "." in them have list datatypes
# no value is marked by NaN
# Sometimes referenced_tweet data is missing. That is because the referenced_tweet may be from a private account

import requests
import setup
import pandas as pd
import datetime
import numpy as np

def main():
    print(searchTwitter(query="Walgreens", timeframe=10))

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
    expansiondf = expansiondf.rename(columns={"id": "referenced_tweets.id", "text": "ref_twt_text",
                                              "created_at": "ref_twt_created_at",
                                              "context_annotations": "ref_twt_context_annotations",
                                              "entities": "ref_twt_entities",
                                              "possibly_sensitive": "ref_twt_possibly_sensitive",
                                              "referenced_tweets": "ref_twt_referenced_tweets"})

    # This section parses out relevant data from Context_annotations, entities, referenced_tweets object columns
    # Referenced_tweets
    df = replaceObjectColumn(df, "referenced_tweets",
                             parseObjectColumn(df, "referenced_tweets",
                                               "referenced_tweets.type", "referenced_tweets.id"))
    # Entities
    df = replaceObjectColumn(df, "entities",
                             parseObjectColumn(df, "entities", "entities.annotations.probability",
                                               "entities.annotations.type", "entities.annotations.normalized_text",
                                               "entities.urls.url", "entities.hashtags.tag",
                                               "entities.mentions.username", "entities.cashtags.tag"))
    # Context_annotations
    df = replaceObjectColumn(df, "context_annotations",
                             parseObjectColumn(df, "context_annotations", "context_annotations.domain.name",
                                               "context_annotations.domain.description",
                                               "context_annotations.entity.name",
                                               "context_annotations.entity.description"))
    # ref_twt_referenced_tweets
    expansiondf = replaceObjectColumn(expansiondf, "ref_twt_referenced_tweets",
                             parseObjectColumn(expansiondf, "ref_twt_referenced_tweets",
                                               "ref_twt_referenced_tweets.type", "ref_twt_referenced_tweets.id"))
    # ref_twt_entities
    expansiondf = replaceObjectColumn(expansiondf, "ref_twt_entities",
                             parseObjectColumn(expansiondf, "ref_twt_entities", "ref_twt_entities.annotations.probability",
                                               "ref_twt_entities.annotations.type", "ref_twt_entities.annotations.normalized_text",
                                               "ref_twt_entities.urls.url", "ref_twt_entities.hashtags.tag",
                                               "ref_twt_entities.mentions.username", "ref_twt_entities.cashtags.tag"))
    # ref_twt_context_annotations
    expansiondf = replaceObjectColumn(expansiondf, "ref_twt_context_annotations",
                             parseObjectColumn(expansiondf, "ref_twt_context_annotations", "ref_twt_context_annotations.domain.name",
                                               "ref_twt_context_annotations.domain.description",
                                               "ref_twt_context_annotations.entity.name",
                                               "ref_twt_context_annotations.entity.description"))

    # This links up the main df and expansiondf by referenced_tweets.id so it's one
    df = df.merge(expansiondf, on="referenced_tweets.id", how="left", suffixes=(None, '_y'))

    # Table Display
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 3000)
    pd.set_option('max_colwidth', 100)

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
                try:
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
                except TypeError:
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
                try:
                    address = args[i].split(".")
                    try:
                        for j in range(len(row[address[1]])):
                            element.append(row[address[1]][j][address[2]])
                        templist.append(element)
                    except KeyError:
                        templist.append(np.nan)
                except TypeError:
                    templist.append(np.nan)
            data.append(templist)
    elif header == "referenced_tweets" or header == "ref_twt_referenced_tweets":
        # Look at the first if statement for the same idea
        # Only difference is that we don't have to check for multiple dicts
        # When we're in the element so the code is simplier
        for i in range(len(args)):
            templist = []
            for row in df.loc[:, header]:
                try:
                    address = args[i].split(".")
                    for j in range(len(row)):
                        element = row[j][address[1]]
                    templist.append(element)
                except TypeError:
                    templist.append(np.nan)
            data.append(templist)
    else:
        raise Exception("Header not recognized")

    # This takes the 2xlen(args) list and enters into a dataframe such that the column headers is args
    # This new dataframe is what will be slapped out to the side of the original df
    # This for loop just to create new titles based on the data domain path
    newdf = pd.DataFrame(index=range(0, len(df.loc[:, header])), columns=args)
    for i in range(len(df.loc[:, header])):
        for j in range(len(args)):
            newdf.loc[i, args[j]] = data[j][i]

    return newdf

# This function replaces the object column from the old dataframe with the parsed dataframe from parseObjectColumn()
# It takes in the old df, the object column to delete, and the df with the new data to concat
def replaceObjectColumn(df, columnheader, parsed_df):
    if type(columnheader) is not str:
        raise Exception ("Second argument of replaceObjectColumn() must be a str")
    else:
        # This section adds the parsed_df at the spot the old objectcolumn was
        indexloc = df.columns.get_loc(columnheader)
        headers = list(parsed_df.columns)
        for i in range(len(parsed_df.columns)):
            df.insert(indexloc+i+1,headers[i], parsed_df.loc[:, headers[i]])
        del df[columnheader]
    return df

# Gets the UTC time var minutes ago in ISO 8601/RFC 3339 format, returns a str
def getOldTime(minutes):
    oldtime = datetime.datetime.utcnow() - datetime.timedelta(minutes=minutes)
    RFC_format = str(oldtime).replace(" ", "T")[:19]
    return RFC_format + "Z"

main()
