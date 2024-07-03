import ast
import logging
import smtplib
import sys
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from helpers.sftpConfig import email_config
from utils.getValueFromTraitsOrProperties import get_authentication_key_from_secret_manager


def auto_email_notification(html_message, subject, to_recipient):

    try:
        port = email_config['PORT']
        smtp_server = email_config['SMTP_SERVER']
        sender_email = email_config['SENDER_EMAIL']
        password = get_authentication_key_from_secret_manager('airflow-config-SMTP_COMPOSER_NOTIFY')
        message = MIMEMultipart("alternative")
        html = MIMEText(html_message, "html")
        message.attach(html)
        message['Subject'] = subject
        message['To'] = to_recipient
        with smtplib.SMTP(smtp_server, port=port) as server:
            server.ehlo()
            server.starttls()
            server.login(sender_email, password)
            server.sendmail(sender_email,  message["To"].split(","), message.as_string())
            server.close()
    except Exception as err:
        logging.exception(f"Unable to send email Notification err: {err}")
        sys.exit(1)


def email_notification_for_mapp(**kwargs):
    """ Triggers an Auto Email for Mapp """

    file_paths = ast.literal_eval(kwargs['file_path'])
    email_file = file_paths[0]
    id_file = file_paths[1]
    remote_path = file_paths[2]
    persona_name = kwargs['job_id'].upper().replace("-", " ")
    logging.info(f"Triggering Email Notification For Mapp, file path {file_paths}, persona_name: {persona_name}")

    html_message = f"""
    <!DOCTYPE html>
    <html>
    <head>
    </head>
    <body>
    <p style="font-family: Calibri">Hi Team,<br><br>
    We have added files for Updation of PG IDs in Mapp system to the below mentioned path for identifier update:<br>
    Path:-<br>
    "{remote_path}" <br><br>
        File names:- <br>
        {email_file}<br>
        {id_file}<br><br>
        Can you please process the same and let us know in case of any queries.<br><br>
        Note: This is an Auto generated Email!<br><br>
        Kind Regards,<br>
        Migration Team</p>
    </body>
    </html>
"""
    email_subject = f"Mapp - Bulk Identifier Update | {persona_name}"
    to_recipient = "pg.cdptwomigration@ltimindtree.com"
    auto_email_notification(html_message, email_subject, to_recipient)
    logging.info("Email Notification Sent, Exiting..")


def email_notification_for_resci(**kwargs):
    """ Triggers an Auto Email for Resci """

    file_paths = ast.literal_eval(kwargs['file_path'])
    remote_path = file_paths[0]
    resci_pg_id_file = file_paths[1]
    persona_name = kwargs['job_id'].upper().replace("-", " ")
    logging.info(f"Triggering Email Notification For Resci, file path {file_paths}, persona_name: {persona_name}")

    html_message = f"""
    <!DOCTYPE html>
    <html>
    <head>
    </head>
    <body>
    <p style="font-family: Calibri">Hi Team,<br><br>
    We have uploaded a file for updating the user ids for {persona_name}. Details are as below:<br>
    Path:- "{remote_path}" <br><br>
        File name:- {resci_pg_id_file}<br><br>
        Can you please process the same and let us know in case of any queries.<br><br>
        Note: This is an Auto generated Email!<br><br>
        Kind Regards,<br>
        Migration Team</p>
    </body>
    </html>
"""
    email_subject = f"Resci - Bulk Identifier Update | {persona_name}"
    to_recipient = "pg.cdptwomigration@ltimindtree.com"
    auto_email_notification(html_message, email_subject, to_recipient)
    logging.info("Email Notification Sent, Exiting..")
