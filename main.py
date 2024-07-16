import requests
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import logging
from dotenv import load_dotenv
import os

load_dotenv(".env")
oauth_file_path = os.getenv('OAUTH_FILE_PATH')
sender_email = os.getenv('SENDER_EMAIL')
sender_password = os.getenv('SENDER_PASSWORD')
receiver_email = os.getenv('RECEIVER_EMAIL')
cc_list = os.getenv('CC_LIST').split(',')
email_subject = f"{os.getenv('EMAIL_SUBJECT')} - {datetime.now().strftime('%Y%m%d')}"
email_body = os.getenv('EMAIL_BODY')


CRED_FILE_PATH = oauth_file_path
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
SPREADSHEET_NAME = "TVPOL Sheets Scraper"
COLUMN_INDEX = 13  # Column M

CURRENT_DATE = datetime.now().strftime("%Y%m%d")

LOG_FILE = f"tvp_scraper_{CURRENT_DATE}.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()


@retry(stop=stop_after_attempt(5), wait=wait_fixed(120),
       retry=retry_if_exception_type(requests.exceptions.RequestException))
def fetch_url(url):
    response = requests.get(url)
    logger.info(f"Processing URL: {url}")
    return response


@retry(stop=stop_after_attempt(5), wait=wait_fixed(120),
       retry=retry_if_exception_type(gspread.exceptions.APIError))
def update_google_sheet(sheet_to_update, data_to_update):
    """Gets A to H columns, change in function if you'd like to add/remove columns."""
    sheet_to_update.batch_clear(["A2:H"])
    batch_update = []

    for i, row in enumerate(data_to_update, start=2):
        batch_update.append({
            "range": f"A{i}:H{i}",
            "values": [row]
        })

    sheet_to_update.batch_update(batch_update)
    logger.info("Sheet updated successfully.")


@retry(stop=stop_after_attempt(5), wait=wait_fixed(120),
       retry=retry_if_exception_type(smtplib.SMTPException))
def send_email_with_attachment(file_path):
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = email_subject
    msg['Cc'] = ', '.join(cc_list)

    msg.attach(MIMEText(email_body, 'plain'))

    with open(file_path, 'rb') as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename= {file_path.name}')
        msg.attach(part)

    with open(LOG_FILE, 'rb') as log_attachment:
        log_part = MIMEBase('application', 'octet-stream')
        log_part.set_payload(log_attachment.read())
        encoders.encode_base64(log_part)
        log_part.add_header('Content-Disposition', f'attachment; filename= {Path(LOG_FILE).name}')
        msg.attach(log_part)

    text = msg.as_string()

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(sender_email, sender_password)
    server.sendmail(sender_email, receiver_email, text)
    server.quit()
    logger.info('Email sent successfully')


def parse_xml_content(xml_content):
    root = ET.fromstring(xml_content)
    records = root.findall('.//prrecord')

    # Logging number of records to check against the output
    logger.info(f"Count of records: {len(records)}")
    parsed_results = []

    for record in records:
        title = record.find('.//TITEL').text
        # Skipping rows with filler programs
        if title == "Zakończenie dnia":
            continue

        tx_day = record.find('.//PR_AIRDATE')
        tx_time = record.find('.//START')
        epg_description = record.find('.//EPG')
        episode_id = record.find('.//PR_CODE')
        prod_year = record.find('.//JAHR')
        rating = record.find('.//PLRATING')
        genre = record.find('.//TEMATYKA')

        if tx_day is None:
            logger.warning("Missing tag: PR_AIRDATE. TX DATE WILL BE EMPTY, CHECK OUTPUT!")
        if tx_time is None:
            logger.warning("Missing tag: START. TX TIME WILL BE EMPTY, CHECK OUTPUT!")
        if epg_description is None:
            logger.warning("Missing tag: EPG. EPG description will be empty.")
        if episode_id is None:
            logger.warning("Missing tag: PR_CODE. Episode ID will be empty.")
        if prod_year is None:
            logger.warning("Missing tag: JAHR. Production year field will be empty.")
        if rating is None:
            logger.warning("Missing tag: PLRATING. Ratings field will be empty.")
        if genre is None:
            logger.warning("Missing tag: TEMATYKA. Genre field will be empty.")

        tx_day_text = tx_day.text if tx_day is not None else ""
        tx_time_text = tx_time.text if tx_time is not None else ""
        epg_description_text = epg_description.text if epg_description is not None else ""
        episode_id_text = episode_id.text if episode_id is not None else ""
        prod_year_text = prod_year.text if prod_year is not None else ""
        rating_text = rating.text if rating is not None else ""
        genre_text = genre.text if genre is not None else ""

        data_row = [
            tx_day_text,
            tx_time_text,
            title,
            epg_description_text,
            episode_id_text,
            prod_year_text,
            rating_text,
            genre_text,
        ]

        parsed_results.append(data_row)

    return parsed_results


def save_data_to_txt(data_to_save, output_dir):
    filename = f"TVPPol_output_{CURRENT_DATE}.txt"
    file_path = output_dir / filename

    if file_path.exists():
        file_path.unlink()
    with file_path.open('w', encoding='utf-16') as file:
        for row in data_to_save:
            file.write('\t'.join(row) + '\n')
    logger.info(f"Data saved to {file_path}")
    return file_path


creds = ServiceAccountCredentials.from_json_keyfile_name(CRED_FILE_PATH, SCOPE)
client = gspread.authorize(creds)
sheet = client.open(SPREADSHEET_NAME).sheet1

urls_list = sheet.col_values(COLUMN_INDEX)[1:]  # Column M
all_results = []

for url in urls_list:
    if url:
        response = fetch_url(url)
        all_results.extend(parse_xml_content(response.content))

update_google_sheet(sheet, all_results)

sheet_data = sheet.get("A2:H")

output_directory = Path.cwd()
saved_file_path = save_data_to_txt(sheet_data, output_directory)

send_email_with_attachment(saved_file_path)
