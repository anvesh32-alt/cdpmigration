import ast
import json
import logging
import sys
import requests

from helpers import constants
from utils.getValueFromTraitsOrProperties import get_authentication_key_from_secret_manager


def get_auth_id_for_job_submission(authorization_token):
    """ The module gets the Auth id of lytics_aws_sftp_auth for the submission of export job in Lytics """

    url = "https://api.lytics.io/v2/auth"
    headers = {
        "accept": "application/json",
        "Authorization": authorization_token
    }

    responses = requests.get(url, headers=headers)
    if responses.status_code == 200:
        logging.info(f'Payload Fetched From Lytics API, processing further..')
        json_response = responses.json()['data']

        for response in json_response:
            try:
                if response['type'] == 'lytics_aws_sftp_auth' and response['status'] == 'healthy' \
                        and response['label'] == 'Data Specialist SFTP Upload':
                    return response['id']
            except KeyError:
                logging.exception(f"Auth Token not Present")
                sys.exit(1)
    else:
        logging.exception(f"Unable to Fetch Auth ID for Lytics File Transfer Service, err: {responses.status_code},"
                          f"{responses.text}")
        sys.exit(1)


def submit_export_job_in_lytics(**kwargs):
    """ Submits an export job in Lytics to export the file that was uploaded to the Lytics SFTP location. """

    # Read all the parameters
    aid_and_file_path = ast.literal_eval(kwargs['aid_and_file_name'])
    account_id = aid_and_file_path[0]
    file_name = aid_and_file_path[1]
    downstream_system = kwargs['downstream_system']

    logging.info(f"Account ID: {account_id}, file name: {file_name}, downstream system: {downstream_system}")
    get_values_from_secret_manager_to_authenticate = (ast.literal_eval(get_authentication_key_from_secret_manager(
        constants.LYTICS_SFTP_SECRET_KEY)))[account_id]
    authorization_token = get_values_from_secret_manager_to_authenticate['auth_token']
    aid_auth_id = get_auth_id_for_job_submission(authorization_token)

    url = f"https://api.lytics.io/v2/job/sftp-import?account_id={account_id}"

    # For Last mapp the stream, fields will change
    if downstream_system == 'lastmapp':
        stream = f"mapp_attributes_{account_id}"
        fields = ["userId", "traits.mappemail"]
    else:
        stream = f"segment_personas_{account_id}"
        fields = ["previousId", "userId", "type"]

    payload = {"config": {"fields": fields, "filename": file_name,
                          "stream": stream}, "auth_ids": [aid_auth_id],  # auth id for the role
               "name": f"Migration Export Job {account_id}"}
    payload_to_str = json.dumps(payload)
    headers = {
        "accept": "application/json",
        "content-type": "*/*",
        "Authorization": authorization_token  # Auth Key
    }
    response = requests.post(url, data=payload_to_str, headers=headers)
    if response.status_code == 201:
        job_id = response.json()['data']['id']
        logging.info(f"The Job has been submitted to Lytics for File Import, "
                     f"job id: {job_id}, job name : {payload['name']}, stream: {stream}")
        return job_id
    else:
        logging.exception(f"Unable to Submit Job For File Import, err: {response.status_code},"
                          f"{response.text}")
        sys.exit(1)


def create_auth_id_for_job_submission(authorization_token, account_id):
    url = f"https://api.lytics.io/v2/auth/lytics-aws-sftp-auth?account_id={account_id}"

    # todo: module is not used as of now, username and password needs to be worked upon.
    payload = {"config": {"user": "", "password": ""},
               "provider_id": "93058311a5dc4c05a9114466d0f5ac3c", "label": "Data Specialist SFTP Upload1",
               "provider_slug": "custom", "type": "lytics_aws_sftp_auth", "account_id": account_id}

    payload_to_str = json.dumps(payload)

    headers = {
        "accept": "application/json",
        "content-type": "*/*",
        "Authorization": authorization_token
    }
    response = requests.post(url, data=payload_to_str, headers=headers)
    if response.status_code == 201:
        auth_id = response.json()['data']['id']
        return auth_id

    else:
        logging.exception(f"Unable to Create Auth ID for Lytics File Transfer Service, err: {response.status_code},"
                          f"{response.text}")
        sys.exit(1)
