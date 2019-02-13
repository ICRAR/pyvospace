import configparser

config=configparser.ConfigParser()

config["ngas"]={"http_servers": ["http://localhost:7777","http://somplace.somewhere:5555"]}

with open("trial.ini","w") as configfile:
    config.write(configfile)
