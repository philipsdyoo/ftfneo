# -*- coding: utf-8 -*-
"""
Fund That Flip - Code Challenge
NASA NEO Collector

Collects data from the NASA NEO API and stores it in a MySQL database
"""

import sys
import time
import re
import argparse
import requests
import configparser
import datetime
import pytz
import mysql.connector
import logging

# Checks the arguments passed through the command line
# Returns a dictionary:
#   pass = True if the date entered passed the format check; False otherwise
#   msg = String message for the pass/failure
def check_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-end_date')
    args = parser.parse_args()
    
    end_date = args.end_date
    if end_date is None:
        return { "pass": False, "msg": "Please include a value for -end_date argument (e.g. -end_date 2020-03-15)", "end_date": end_date }
    
    if check_date_format(end_date):
        return { "pass": True, "msg": "End date entered: "+end_date, "end_date": end_date }
    else:
        return { "pass": False, "msg": "The value entered for -end_date argument is not of the correct format: YYYY-MM-DD (e.g. 2020-03-15)", "end_date": end_date }

# Checks if the date string matches the regex format specified - default: YYYY-MM-DD
# Returns true if the date format is satisfied; otherwise, false
def check_date_format(date, regex_format="\d{4}-\d{2}-\d{2}"):
    return bool(re.match(regex_format, date))

# Checks if data collection and loading is already happening
# Returns a dictionary:
#   running = True if collection is already running; False otherwise
#   start_dt = the start datetime of the last collection
#   end_dt = the end datetime of the last collection
def check_running(db):
    conn = mysql.connector.connect(
        host = db["host"],
        user = db["user"],
        password = db["password"],
        database = db["database"]
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

# Sets the record in neo_load to running if running_bool is True; otherwise, sets to not running if running_bool is False
def set_running(db, running_bool):
    conn = mysql.connector.connect(
        host = db["host"],
        user = db["user"],
        password = db["password"],
        database = db["database"]
    )
    cursor = conn.cursor()
    
    try:
        logging.info("Setting running as " + str(running_bool))
        query = "UPDATE neo_load SET running = FALSE, end_dt = %s WHERE id = 1"
        if running_bool:
            query = "UPDATE neo_load SET running = TRUE, start_dt = %s WHERE id = 1"
        now_ts = int(datetime.datetime.now().astimezone(pytz.UTC).timestamp())
        now_dt = datetime.datetime.utcfromtimestamp(now_ts)
        value = (now_dt , )
        cursor.execute(query, value)
        conn.commit()
    except Exception as e:
        logging.error(e)
    finally:
        cursor.close()
        conn.close()

# Empties the neo table by dropping it and recreating it
# Side note: The other method could be to just truncate the table instead
def empty_table(db):
    conn = mysql.connector.connect(
        host = db["host"],
        user = db["user"],
        password = db["password"],
        database = db["database"]
    )
    cursor = conn.cursor()
    
    try:
        logging.info("Emptying the table by dropping and recreating it")
        drop_query = "DROP TABLE IF EXISTS neo"
        cursor.execute(drop_query)
        create_query = """
            CREATE TABLE IF NOT EXISTS neo (
        	id INT NOT NULL AUTO_INCREMENT,
        	neo_reference_id VARCHAR(4) NOT NULL,
        	name VARCHAR(100) NOT NULL,
        	nasa_jpl_url VARCHAR(100) NOT NULL,
        	absolute_magnitude_h DECIMAL(10, 2) NOT NULL,
        	is_potentially_hazardous_asteroid BOOLEAN NOT NULL,
        	is_sentry_object BOOLEAN NOT NULL,
        	estimated_diameter_min DECIMAL(20, 10) NOT NULL,
        	estimated_diameter_max DECIMAL(20, 10) NOT NULL,
        	close_approach_datetime DATETIME NOT NULL,
        	relative_velocity DECIMAL(30, 10) NOT NULL,
        	miss_distance DECIMAL(30, 10) NOT NULL,
        	orbiting_body VARCHAR(100) NOT NULL,
        	PRIMARY KEY(id)
        )"""
        cursor.execute(create_query)
        conn.commit()
    except Exception as e:
        logging.error(e)
    finally:
        cursor.close()
        conn.close()

# Makes an HTTP GET request to the url
# Returns a dictonary:
#   good = True if the status code is 200; False otherwise
#   response = the response body translated from json to a dictionary
def make_request(url):
    res = requests.get(url)
    res_dict = res.json()
    if res.status_code == 200:
        return { "good": True, "response": res_dict }
    else:
        return { "good": False, "response": res_dict }

# Iterates over the data and stores the record values in a tuple which gets added to a list of tuples
# Data entries from a date after the specified end date are skipped
# Returns a list of tuples which each contain the record values to be inserted into the neo database
def get_values_list(data, end_date):
    values_list = []
    dates = data["near_earth_objects"]
    for date in dates:
        # Skip dates that are past the end date
        if datetime.datetime.fromisoformat(date) > datetime.datetime.fromisoformat(end_date):
            continue
        for neo in dates[date]:
            # Store only the last 4 digits of the NEO reference ID
            # Commented out line is if one wants to pad the string with X's in case there are fewer than 4 digits in the ID
            #neo_reference_id = neo["neo_reference_id"][-4:].rjust(4, "X")
            neo_reference_id = neo["neo_reference_id"][-4:]
            name = neo["name"]
            nasa_jpl_url = neo["nasa_jpl_url"]
            absolute_magnitude_h = neo["absolute_magnitude_h"]
            is_potentially_hazardous_asteroid = neo["is_potentially_hazardous_asteroid"]
            is_sentry_object = neo["is_sentry_object"]
            # Grabs only the imperial units, specifically miles
            estimated_diameter_min = neo["estimated_diameter"]["miles"]["estimated_diameter_min"]
            estimated_diameter_max = neo["estimated_diameter"]["miles"]["estimated_diameter_max"]
            # Assumes only one entry for close_approach_data, so take only the first element
            # Stores the epoch as a GMT timestamp
            close_approach_datetime = datetime.datetime.utcfromtimestamp(neo["close_approach_data"][0]["epoch_date_close_approach"]/1000)
            relative_velocity = neo["close_approach_data"][0]["relative_velocity"]["miles_per_hour"]
            miss_distance = neo["close_approach_data"][0]["miss_distance"]["miles"]
            orbiting_body = neo["close_approach_data"][0]["orbiting_body"]
            values = (neo_reference_id, name, nasa_jpl_url, absolute_magnitude_h, is_potentially_hazardous_asteroid, is_sentry_object, estimated_diameter_min, estimated_diameter_max, close_approach_datetime, relative_velocity, miss_distance, orbiting_body)
            values_list.append(values)
    return values_list

# Takes the list of values tuples and inserts them into the neo table
def insert_into_db(values_list, db):
    conn = mysql.connector.connect(
        host = db["host"],
        user = db["user"],
        password = db["password"],
        database = db["database"]
    )
    cursor = conn.cursor()
    try:
        query = """
        INSERT INTO neo
        (neo_reference_id, name, nasa_jpl_url, 
        absolute_magnitude_h, is_potentially_hazardous_asteroid, is_sentry_object,
        estimated_diameter_min, estimated_diameter_max, 
        close_approach_datetime, relative_velocity, miss_distance, orbiting_body)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        
        logging.info("Inserting "+str(len(values_list))+" records...")
        cursor.executemany(query, values_list)
        conn.commit()
    except Exception as e:
        logging.error(e)
    finally:
        cursor.close()
        conn.close()

# Takes the start_date and end_date and collects data from the NEO API between those dates inclusively
# The API requires the api_key
# db is a dictionary that holds database connection information
def collect_and_store(start_date, end_date, api_key, db):
    
    # Empty the table and set the collection running status to true
    empty_table(db)
    set_running(db, True)
    
    current_date = start_date
    req_counter = 1
    while datetime.datetime.fromisoformat(current_date) <= datetime.datetime.fromisoformat(end_date):
        # detailed is set to false for this implementation; if true, orbital data is included
        url = "https://api.nasa.gov/neo/rest/v1/feed?start_date="+current_date+"&detailed=false&api_key="+api_key
        logging.info("Request #"+str(req_counter)+": "+url)
        
        res = make_request(url)
        
        if res["good"]:
            values_list = get_values_list(res["response"], end_date)
            insert_into_db(values_list, db)
            
            # Set the new start_date by incrementing by 8 days (since the result actually contains 8 days when an end_date is not provided)
            new_current_dt = datetime.datetime.fromisoformat(current_date) + datetime.timedelta(days = 8)
            current_date = new_current_dt.strftime("%Y-%m-%d")
            
            # API calls are restricted to 1000 every hour - sleep to avoid hitting the limit
            time.sleep(5)
        else:
            logging.error("Request failed: ", url)
        req_counter += 1
    
    set_running(db, False)

# The main function which gets run when the script is run
def main():
    
    # Logs to a log file and also outputs to the console/standard output
    logging.basicConfig(handlers = [logging.FileHandler("neo.log"), logging.StreamHandler(sys.stdout)], format="%(asctime)s|%(levelname)s|%(message)s", level=logging.INFO)
    
    # Check the arguments pass through the command line
    # Expects -end_date YYYY-MM-DD
    check_args_res = check_args()
    if check_args_res["pass"]:
        logging.info(check_args_res["msg"])
    else:
        logging.error(check_args_res["msg"])
        sys.exit()
    
    # Fetches data from the start date to the end date inclusively
    start_date = "1982-12-10"
    end_date = check_args_res["end_date"]
    
    # Fetch the configuration values from the config file
    config = configparser.ConfigParser()
    config.read("config.ini")
    
    api_key = config.get("API", "api_key")
    
    host = config.get("DATABASE", "host")
    user = config.get("DATABASE", "user")
    password = config.get("DATABASE", "password")
    database = config.get("DATABASE", "database")
    
    db = { "host": host, "user": user, "password": password, "database": database }
    
    # Check if collection is already running. If so, exit the script. Otherwise, continue.
    check_running_res = check_running(db)
    
    if check_running_res["running"]:
        logging.error("The load is already running. It started " + check_running_res["start_dt"] + ". Please try again later.")
        sys.exit()
        
    # Begin collecting NEO data
    collect_and_store(start_date, end_date, api_key, db)
    logging.info("Collection completed.")

if __name__ == '__main__':
    main()