import time
import csv
import os
import requests
import logging
from datetime import datetime, timedelta
from datetime import datetime, timezone
import json
import glob

logging.basicConfig(level=logging.INFO)

def load_config(config_file='credentials.json'):
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        logging.error(f"Configuration file '{config_file}' not found.")
        exit(1)
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from the configuration file: {e}")
        exit(1)

# Load settings from the JSON configuration file
config = load_config()

EXPECTED_FILE_CHECKLIST = r'E:\ZeftPay_Payliance\CHECK_LIST.csv'

NEW_RELIC_API_KEY = config.get("NEW_RELIC_API_KEY")
NEW_RELIC_ACCOUNT_ID = config.get("NEW_RELIC_ACCOUNT_ID")
INPUT_FOLDER = config.get("INPUT_FOLDER")
ARCHIVE_FOLDER = config.get("ARCHIVE_FOLDER")
ERROR_FOLDER = config.get("ERROR_FOLDER")
INPUT_FOLDER2 = config.get("INPUT_FOLDER2")  # Add second input folder
ARCHIVE_FOLDER2 = config.get("ARCHIVE_FOLDER2")  # Add second archive folder
ERROR_FOLDER2 = config.get("ERROR_FOLDER2")  # Add second error folder

# Constants for time calculation
TIME_BEFORE = timedelta(minutes=2)
TIME_AFTER = timedelta(minutes=2)
IN_PROGRESS_THRESHOLD = timedelta(seconds=15)

# Batch size for events to send to New Relic
BATCH_SIZE = 1

EXCLUDE_MISSING_ON_DAYS = {
    'CRMD3375.': [6],# Don't trigger Missing for on Sundays (day 6)
    'CRMD3360.': [6],
    'CRMD3358.': [6],
    'CRMD3357.': [6],
    'CCBD3076.': [5],# Don't trigger Missing for on Saturdays (day 5)
    'CCBD3077.': [5] 
}

def replace_date_tokens(file_name, current_time):
    """
    Replaces the date tokens <dateToken>, <dateToken1>, and <dateToken2> in the file name
    with the corresponding date formats.
    """
    for token, date_format in DATE_TOKENS.items():
        formatted_date = current_time.strftime(date_format)
        file_name = file_name.replace(token, formatted_date)
    return file_name


def send_batch_to_new_relic(batch):
    """
    Sends a batch of events to New Relic.
    """
    if not batch:
        return

    url = f"https://insights-collector.newrelic.com/v1/accounts/{NEW_RELIC_ACCOUNT_ID}/events"
    headers = {
        'Api-Key': NEW_RELIC_API_KEY,
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.post(url, json=batch, headers=headers)
        response.raise_for_status()
        logging.info(f"Batch of {len(batch)} events sent to New Relic.")
        for event in batch:
            logging.info(f"Event sent: {json.dumps(event)}")  # Log the event details after sending.
    except requests.exceptions.HTTPError as err:
        logging.error(f"Failed to send batch to New Relic: {err} (Status code: {response.status_code})")
    time.sleep(1)

def read_expected_files():
    expected_files = []
    try:
        with open(EXPECTED_FILE_CHECKLIST, mode='r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                expected_files.append(row)
    except Exception as e:
        logging.error(f"Error reading expected files: {e}")
    return expected_files

DATE_TOKENS = {
    '<dateToken>': "%m%d%Y",  # Example: 11122024
    '<dateToken1>': "%Y%m%d",  # Example: 20241112
    '<dateToken2>': "%Y%d%m"   # Example: 20241211
}

def count_file_occurrences(expected_file_name, input_files, archive_files, error_files):
    all_files = set(input_files.keys()) | archive_files | error_files
    occurrence_count = 0

    for file in all_files:
        if expected_file_name in file:
            occurrence_count += 1
    return occurrence_count

def monitor_local_folders():
    expected_files = read_expected_files()
    received_files = set()
    in_progress_files = {}
    file_timestamps = {}
    found_files = set()
    reparsed_files = set()
    processed_error_files = set()
    error_files_reported = set()
    missing_files_reported = set()

    event_batch = []

    while True:
        try:
            current_time = datetime.now()
            logging.info(f"Current time: {current_time}")

            # List files in all input, archive, and error directories for .txt, .csv, and .ADFO files
            input_files = {
                os.path.basename(f): os.path.getmtime(f) 
                for f in glob.glob(os.path.join(INPUT_FOLDER, '*.txt')) +
                glob.glob(os.path.join(INPUT_FOLDER, '*.csv')) +
                glob.glob(os.path.join(INPUT_FOLDER, '*.ADFO')) +  # Added .ADFO files
                glob.glob(os.path.join(INPUT_FOLDER2, '*.txt')) +
                glob.glob(os.path.join(INPUT_FOLDER2, '*.csv')) +
                glob.glob(os.path.join(INPUT_FOLDER2, '*.ADFO'))  # Added .ADFO files
            }

            archive_files = {
                os.path.basename(f) 
                for f in glob.glob(os.path.join(ARCHIVE_FOLDER, '*')) + 
                glob.glob(os.path.join(ARCHIVE_FOLDER2, '*')) 
            }

            error_files = {
                os.path.basename(f) 
                for f in glob.glob(os.path.join(ERROR_FOLDER, '*')) + 
                glob.glob(os.path.join(ERROR_FOLDER2, '*'))
            }

            for expected in expected_files:
                expected_file_name = replace_date_tokens(expected['fileName'], current_time).strip()
                expected_time = datetime.strptime(expected['expectedTime'], '%H:%M').replace(year=current_time.year, month=current_time.month, day=current_time.day)
                category = expected.get('category', 'General')
                client = expected.get('client', 'Unknown')
                expected_occurrences = int(expected.get('expectedOccurrences', 1))
                time_before = expected_time - TIME_BEFORE
                time_after = expected_time + TIME_AFTER

                # Check for found files within the time range (Received)
                if time_before <= current_time <= time_after:
                    matching_files = [file for file in input_files if expected_file_name in file and (file.endswith('.txt') or file.endswith('.csv') or file.endswith('.ADFO'))]

                    for expected_file in matching_files:
                        if expected_file_name not in processed_error_files and expected_file_name not in received_files:
                            logging.info(f"File {expected_file} matched the pattern '{expected_file_name}' from expected list.")
                            event = ({
                                'eventType': 'Received',
                                'clientName': expected['client'],
                                'status': "Received",
                                'fileName': expected_file,
                                'category': category,
                                'expectedTime': expected_time.strftime('%Y-%m-%d %H:%M:%S'),
                                'timestamp': datetime.now(timezone.utc).isoformat()
                            })
                            event_batch.append(event)
                            logging.info(f"Event generated for {expected_file}:")  # Log event details
                            received_files.add(expected_file)
                            found_files.add(expected_file)
                            file_timestamps[expected_file] = input_files[expected_file]
                            
                   # **Missing File Check - Trigger near the end of time_after window Until then it will keep checking for expected file**
                    if expected_file_name not in missing_files_reported:
                        if expected_time <= current_time <= time_after: 
                            occurrence_count = count_file_occurrences(expected_file_name, input_files, archive_files, error_files)
                            logging.info(f"Occurrences of '{expected_file_name}' across all folders: {occurrence_count}")
                            if current_time >= (time_after - IN_PROGRESS_THRESHOLD):
                                if occurrence_count < expected_occurrences:
                                    today_weekday = current_time.weekday()  # 0=Monday, ..., 5=Saturday, 6=Sunday
                                    for excluded_filename, excluded_days in EXCLUDE_MISSING_ON_DAYS.items():
                                        if excluded_filename in expected_file_name and today_weekday in excluded_days:
                                            logging.info(f"Skipping Missing event for file '{expected_file_name}' on {current_time.strftime('%A')}")
                                            break  # Skip the event for this file and stop checking further
                                    else:
                                        event_batch.append({
                                            'eventType': 'Missing',
                                            'clientName': expected['client'],
                                            'status': "Missing",
                                            'fileName': expected_file_name,
                                            'category': category,
                                            'expectedTime': expected_time.strftime('%Y-%m-%d %H:%M:%S'),
                                            'timestamp': datetime.now(timezone.utc).isoformat()
                                        })
                                        missing_files_reported.add(expected_file_name)

                # **In Progress** - Trigger when file has been in the input folder for too long
                for file_name in list(input_files):
                    if file_name not in in_progress_files:
                        # Add the file to the in_progress_files dict when it first appears
                        in_progress_files[file_name] = current_time
                    elif current_time - in_progress_files[file_name] >= IN_PROGRESS_THRESHOLD:
                        # Check if file has been in progress for longer than threshold
                        expected_file_details = next((exp for exp in expected_files if replace_date_tokens(exp['fileName'], current_time).strip() in file_name), None)
                        if expected_file_details:
                            client = expected_file_details.get('client', 'Unknown')
                            category = expected_file_details.get('category', 'General')

                            event = ({
                                'eventType': 'Inprogress',
                                'clientName': client,
                                'status': "In Progress",
                                'fileName': file_name,
                                'category': category,
                                'expectedTime': expected_time.strftime('%Y-%m-%d %H:%M:%S'),
                                'timestamp': datetime.now(timezone.utc).isoformat()
                            })
                            event_batch.append(event)
                            logging.info(f"Event generated for In Progress file: {expected_file}")  # Log event details
                        # Update the timestamp when file is still in progress
                        in_progress_files[file_name] = current_time

                # New Received event for early or late files (before time_before or after time_after)
                if current_time < time_before or current_time > time_after:
                    matching_files = [file for file in input_files if expected_file_name in file and (file.endswith('.txt') or file.endswith('.csv') or file.endswith('.ADFO'))]
                    for expected_file in matching_files:
                        if expected_file_name not in processed_error_files and expected_file_name not in received_files:
                            logging.info(f"File {expected_file} matched the pattern '{expected_file_name}' from expected list.")
                            event = ({
                                'eventType': 'Received',
                                'clientName': expected['client'],
                                'status': "Received",
                                'fileName': expected_file,
                                'category': category,
                                'expectedTime': expected_time.strftime('%Y-%m-%d %H:%M:%S'),
                                'timestamp': datetime.now(timezone.utc).isoformat()
                            })
                            received_files.add(expected_file)
                            found_files.add(expected_file)
                            file_timestamps[expected_file] = input_files[expected_file]

                # Inside the loop where you check for files moved to the archive folder
                for file_name in list(received_files):
                    if file_name in archive_files:
                        expected_file_details = next((exp for exp in expected_files if replace_date_tokens(exp['fileName'], current_time).strip() in file_name), None)
                        if expected_file_details:
                            client = expected_file_details.get('client', 'Unknown')
                            category = expected_file_details.get('category', 'General')

                            event_batch.append({
                                'eventType': 'Completed',
                                'clientName': client,
                                'status': "Completely Parsed",
                                'fileName': file_name,
                                'category': category,
                                'expectedTime': expected_time.strftime('%Y-%m-%d %H:%M:%S'),
                                'timestamp': datetime.now(timezone.utc).isoformat()
                            })
                        received_files.remove(file_name)
                        found_files.discard(file_name)

                # Inside the loop where you check for files moved to the error folder
                for file_name in list(received_files):
                    if file_name in error_files:
                        expected_file_details = next((exp for exp in expected_files if replace_date_tokens(exp['fileName'], current_time).strip() in file_name), None)
                        if expected_file_details:
                            client = expected_file_details.get('client', 'Unknown')
                            category = expected_file_details.get('category', 'General')

                            event_batch.append({
                                'eventType': 'Error',
                                'clientName': client,
                                'status': "Error while Parsing",
                                'fileName': file_name,
                                'category': category,
                                'expectedTime': expected_time.strftime('%Y-%m-%d %H:%M:%S'),
                                'timestamp': datetime.now(timezone.utc).isoformat()
                            })
                        received_files.remove(file_name)
                        found_files.discard(file_name)
                        error_files_reported.add(file_name)
                        processed_error_files.add(file_name)
                        
                for file_name in error_files_reported:
                    if file_name in input_files:
                        if file_name not in reparsed_files:
                            expected_file_details = next((exp for exp in expected_files if replace_date_tokens(exp['fileName'], current_time).strip() in file_name), None)
                            if expected_file_details:
                                client = expected_file_details.get('client', 'Unknown')
                                category = expected_file_details.get('category', 'General')
                                event_batch.append({
                                    'eventType': 'Reparsing',
                                    'clientName': client,
                                    'status': "Reparsed File",
                                    'fileName': file_name,
                                    'category': category,
                                    'expectedTime': expected_time.strftime('%Y-%m-%d %H:%M:%S'),
                                    'timestamp': datetime.now(timezone.utc).isoformat()
                                })
                for file_name in error_files_reported:
                    if file_name in input_files:
                        if file_name not in reparsed_files:
                            expected_file_details = next((exp for exp in expected_files if replace_date_tokens(exp['fileName'], current_time).strip() in file_name), None)
                            if expected_file_details:
                                client = expected_file_details.get('client', 'Unknown')
                                category = expected_file_details.get('category', 'General')
                                event_batch.append({
                                    'eventType': 'Reparsingfile',
                                    'clientName': client,
                                    'status': "Reparsed",
                                    'fileName': file_name,
                                    'category': category,
                                    'expectedTime': expected_time.strftime('%Y-%m-%d %H:%M:%S'),
                                    'timestamp': datetime.now(timezone.utc).isoformat()
                                })
                                reparsed_files.add(file_name)
                                received_files.add(file_name)
                    
            if len(event_batch) >= BATCH_SIZE:
                send_batch_to_new_relic(event_batch)
                event_batch = []

            time.sleep(20)

        except KeyboardInterrupt:
            logging.info("Monitoring stopped by user.")
            break
        except Exception as e:
            logging.error(f"Error in monitoring process: {e}")
            
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    monitor_local_folders()