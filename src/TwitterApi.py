# Steps for jchen to use envfile
# 1. download envfile extension from the pycharm plugins section
# 2. put the actual .env file in your pycharm but ENSURE you do not committ it at all. Add it to gitignore asap
# 3. import the setup.py file whenever you need stuff from the env file. Make sure you take a look at it first.
# 4. Go to Run -> Edit Configurations -> Enable EnvFile -> The Plus Button in the box and add the env file
# !!! You MUST do step 4 for EVERY file you use the info in, this means it needs to be done in both setup.py AND this file.

import setup

print(setup.twitterApiKey) #this is a test to see if rits working