
"""
@author: Prateek Verma

"""

import os  # For system path
from logger import log  # For logging
from configparser import RawConfigParser  # For loading config file
from helper_functions import *  # Importing all the functions from helper_functions

# Global Variable to store config object
config = None


def load_config():
    """Loads config file
    Return(s):
        config (RawConfigParser)    :   RawConfigParser object
        Prateek Verma
    """
    # Accessing config variable
    global config
    try:
        log.info("loading Config file.")
        # Loading config file
        config = RawConfigParser()
        config.read("config.cfg")
        log.info("Config file loaded successfully.")
    except Exception as e:
        log.error(f"Error in loading config file : {str(e)}.")
    return config


def main():
    """Controller function controlling the whole process
    Return(s):
        True (bool) :   If all the steps gets executed successfully
        Prateek Verma
    """
    try:
        log.info("Extracting xml source file url")
        # Extracting the source xml file url (Prateek Verma)
        url = config.get("sourcefile", "xml_source_url")

        log.info("Extracting csv file path")
        csv_path = os.path.join(os.getcwd(), config.get("csv", "csv_path"))

        log.info("Extracting xml file download path")
        # Extracting the download path and creating absolute path (Prateek Verma)
        download_path = os.path.join(
            os.getcwd(), config.get("download", "download_path")
        )

        log.info("Extracting AWS s3 bucket resource information")
        # Extracting the required s3 information from config (Prateek Verma)
        bucket_name = config.get("aws", "bucket_name")
        aws_access_key_id = config.get("aws", "aws_access_key_id")
        aws_secret_access_key = config.get("aws", "aws_secret_access_key")
        region_name = config.get("aws", "region_name")

        log.info("Calling download function")
        # Calling the download helper function to download the file (Prateek Verma)
        xml_file = download(url, download_path, "sourcefile.xml")

        # Checking if the file download failed (Prateek Verma)
        if not xml_file:
            print("File Download Fail, Kindly check logs for more details")
            print("Exiting...")
            return

        log.info("Calling parse_source_xml function")
        # Calling the source xml file parser helper function to download the file (Prateek Verma)
        file_metadata = parse_source_xml(xml_file)

        # Checking if the required file metadata extraction failed (Prateek Verma)
        if not file_metadata:
            print("File Parsing Failed, Kindly check logs for more details")
            print("Exiting...")
            return

        # Extracting file name and file download link from file file_metadata
        filename, file_download_link = file_metadata

        log.info("Calling download function")
        # Calling the download helper function to download the file (Prateek Verma)
        xml_zip_file = download(file_download_link, download_path, filename)

        if not unzip_file(xml_zip_file, download_path):
            print("Extration Failed, Kindly check logs for more details")
            print("Exiting...")
            return

        # Creating absolute path to xml file (Prateek Verma)
        xml_file = os.path.join(download_path, filename.split(".")[0] + ".xml")

        log.info("Calling create csv function")
        # Calling helper function to create csv file (Prateek Verma)
        csv_file = create_csv(xml_file, csv_path)

        if not csv_file:
            print("XML-CSV Conversion Failed, Kindly check logs for more details")
            print("Exiting...")
            return

        status = aws_s3_upload(
            csv_file, region_name, aws_access_key_id, aws_secret_access_key, bucket_name
        )
        if not status:
            print("CSV file upload Failed, Kindly check logs for more details")
            print("Exiting...")
            return

        return True

    except Exception as e:
        log.error(f"Error in loading config file : {str(e)}.")


if __name__ == "__main__":
    # Check for the config file loading (Prateek Verma)
    if not load_config():
        print("Error while loading config file, check logs")
        print("Exiting...")
        # Exiting the script if the config files were not loaded
        exit(1)

    print("Execution started...")
    if main():
        print("Execution completed successfully...")
    else:
        print("Execution Failed!!! Check logs for more details")
