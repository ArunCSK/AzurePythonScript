import subprocess

subprocess.run(['curl -n -F filedata=@"/home/admin1/Desktop/tweets.py" \
-F path="/docs/tweet_usingpyscript.py" \
-F overwrite=true https://eastus.azuredatabricks.net/api/2.0/dbfs/put'])

