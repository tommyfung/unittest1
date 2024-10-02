import json
import logging
import os
import calendar
from flask import Flask, request, jsonify
from google.auth import default
from azure.identity import ClientSecretCredential
from datetime import datetime, timedelta
from get_files import get_files
from send_files import send_files
from rename import rename_files
from zip_files import zip_files
from archive import archive_files

# Load configuration from config.json
with open('config.json') as config_file:
    config = json.load(config_file)

use = config['use']

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

def get_last_working_day(year, month):
   # Get the last day of the month
   last_day = calendar.monthrange(year, month)[1]
   last_date = datetime(year, month, last_day)
   # Check if the last day is a weekend
   if last_date.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
       # If it's a weekend, move back to the previous Friday
       last_date -= timedelta(days=last_date.weekday() - 4)
   return last_date
   
def last_working_day_of_last_month(date):
   # Get the first day of the current month
   first_day_of_current_month = date.replace(day=1)
   # Get the last day of the previous month
   last_day_of_last_month = first_day_of_current_month - timedelta(days=1)
   # Find the last weekday of the previous month
   while last_day_of_last_month.weekday() > 4:  # 0 is Monday, 4 is Friday
       last_day_of_last_month -= timedelta(days=1)
   return last_day_of_last_month

@app.route('/external_file_transfer', methods=['POST'])
def external_file_transfer():
    logging.info('HTTP request received.')

    if use == 'gcp':
    
        action = request.json.get('action')
        if not action:
            return jsonify({"error": "Please provide an action parameter."}), 400
        
        batch_date = request.json.get('date')
        if batch_date:
            try:
                # Validate the date format
                date = datetime.strptime(batch_date, '%Y%m%d')
            except ValueError:
                return jsonify({"error": "Invalid date format. Please use YYYYMMDD."}), 400
        else:
            now = datetime.today()
            if action == 'send':
                last_working_day = get_last_working_day(now.year, now.month)
                if now.date() == last_working_day.date():
                    batch_date = now.strftime('%Y%m%d')
                else:
                    logging.info("Not the batch date, skip.")
                    return jsonify({"warning": "Skip, due to not the batch date."})
            elif action == 'get':
                batch_date = last_working_day_of_last_month(now)
                batch_date = now.strftime('%Y%m%d')

        logging.info(f'The batch date is: {batch_date}')
  
        try:
            # GCP credentials from default environment
            credentials, project = default()
        
            if action == 'send':
                rename_files(config, credentials)
                result = send_files(config, credentials, batch_date)
                return jsonify({"result": result})
           
            elif action == 'get':
                result = get_files(config, credentials, batch_date)
                return jsonify({"result": result})
            
            elif action == 'zip':
                result = zip_files(config, batch_date)
                archive_files(config, credentials, batch_date)
                return jsonify({"result": result})
                        
            else:
                logging.error("Invalid action. Use 'get', 'zip' or 'send'.")
                return jsonify({"error": "Invalid action. Use 'get', 'zip' or 'send'."}), 400
       
        except Exception as e:
            logging.error(f"An error occurred: {e}", exc_info=True)
            return jsonify({"error": f"An error occurred: {e}"}), 500

    return jsonify({"error": "Invalid configuration. Use 'azure' or 'gcp'."}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
