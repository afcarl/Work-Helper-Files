import yaml


''' CHANGE THIS TO YOUR PATH '''
input = "/Users/solidfire/PycharmProjects/Solidfire_DU_Database/credentials.txt"

with open(input, 'r') as f:
    doc = yaml.safe_load(f)

fburl = doc["fogbugz"]["url"]
fbuser = doc["fogbugz"]["username"]
fbpassword = doc["fogbugz"]["password"]

jiraserver = doc["jira"]["server"]
jirauser = doc["jira"]["username"]
jirapassword = doc["jira"]["password"]

sftoken = doc["salesforce"]["token"]
sfreport = doc["salesforce"]["report"]
sfuser = doc["salesforce"]["username"]
sfpassword = doc["salesforce"]["password"]

mySQLuser = doc["mySQL"]["user"]
mySQLpassword = doc["mySQL"]["password"]
mySQLhost= doc["mySQL"]["host"]
mySQLdatabase = doc["mySQL"]["database"]
mySQLraise_on_warnings = doc["mySQL"]["raise_on_warnings"]

hivehost = doc["hive"]["host"]
hiveauth = doc["hive"]["auth"]
hiveuser = doc["hive"]["user"]
hivepassword = doc["hive"]["password"]
hivedatabase = doc["hive"]["database"]

maximum = doc["maxresult"]

f.close()