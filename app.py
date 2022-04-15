# -*- coding: utf-8 -*-
"""
Fund That Flip - Code Challenge
NASA NEO Flask REST API

The REST API that interacts with the NEO collection Python script
"""
from flask import Flask, request
import subprocess
import json
import configparser
import mysql.connector
import logging
import sys

app = Flask(__name__)

# GET request just to check if the server is online
@app.route("/online", methods=["GET"])
def check():
    result = { "status": "Online" }
    return json.dumps(result)

# POST request to start up the neo.py script which will collect NEO data from the NASA API
@app.route("/collect",methods = ["POST"])
def collect():
    end_date = request.form["end_date"]
    # This will not return a response until the script completes (commented out)
    #os.system("python neo.py -end_date " + end_date)
    
    # Check if collection is already running
    check_running_res = check_running()
    
    if check_running_res["running"]:
        # Return a 400 error if load is already running
        error_msg = "The load is already running. It started " + check_running_res["start_dt"] + ". Please try again later."
        logging.error(error_msg)
        return { "status": error_msg }, 400
    else:
        # Otherwise return 200 status code
        # This will let the script run in the background and returns a response right away
        subprocess.Popen(["python","neo.py","-end_date",end_date])
        result = { "status": "Request to collect and store submitted" }
        return result

# Checks if data collection and loading is already happening
# Returns a dictionary:
#   running = True if collection is already running; False otherwise
#   start_dt = the start datetime of the last collection
#   end_dt = the end datetime of the last collection
def check_running():
    # Fetch the configuration values from the config file
    config = configparser.ConfigParser()
    config.read("config.ini")
    
    host = config.get("DATABASE", "host")
    user = config.get("DATABASE", "user")
    password = config.get("DATABASE", "password")
    database = config.get("DATABASE", "database")
    
    conn = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
        database = database
    )
    cursor = conn.cursor()
    
    running_bool = True
    start_dt = None
    end_dt = None
    
    try:
        query = "SELECT running, start_dt, end_dt FROM neo_load"
        cursor.execute(query)
        result = cursor.fetchone()
        running = result[0]
        running_bool = running == 1
        start_dt = result[1]
        end_dt = result[2]
    except Exception as e:
        logging.error(e)
    finally:
        cursor.close()
        conn.close()
    
    return { "running": running_bool, "start_dt": start_dt.strftime("%Y-%m-%d %H:%M:%S"), "end_dt": end_dt.strftime("%Y-%m-%d %H:%M:%S") }

if __name__ == '__main__':
    logging.basicConfig(handlers = [logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)], format="%(asctime)s|%(levelname)s|%(message)s", level=logging.INFO)
    app.run()