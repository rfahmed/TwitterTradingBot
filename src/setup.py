import os

#config for twitter
twitterApiKey = os.environ.get('TWITTER_API_KEY')
twitterSecretKey = os.environ.get('TWITTER_SECRET_KEY')
twitterBearerToken = os.environ.get('TWITTER_BEARER_TOKEN')

print(twitterApiKey) #this is a test to see if its working