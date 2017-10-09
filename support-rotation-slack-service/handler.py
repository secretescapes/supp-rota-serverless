import boto3
import botocore
import datetime
import json
import logging
import os
import sys
import time
from boto3.dynamodb.conditions import Key, Attr


here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "./vendored"))

import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REPLIER_LAMBDA_NAME = os.environ['REPLIER_LAMBDA_NAME']
SLACK_TOKEN = os.environ['SLACK_TOKEN']
STAGE = os.environ['STAGE']
REGION = os.environ['REGION']

unknown_error_message = ":dizzy_face: Something went really wrong, sorry"

_lambda = boto3.client('lambda')

def support_rotation(event, context):
    try:
        logger.info("Support rotation invoked with event: %s" % event)
        params = event.get('body')
        _validate(params['token'])
        text = params.get('text').split()
        response_url = params.get('response_url')
        logger.info("Command received: %s" % text)
        payload = {'text' : text, 'response_url' : response_url, 'username':params.get('user_name'), 'user_id': params.get('user_id')}


        _lambda.invoke(
            FunctionName=REPLIER_LAMBDA_NAME,
            InvocationType='Event',
            Payload=json.dumps(payload))

        return {"text":'Got it!'}
    except Exception as e:
        logger.error("Exception: %s" % e)
        return {"text": unknown_error_message}

def dispatcher(event, context):

    try:
        logger.info("Dispatcher invoked with event: %s" % event)
        text = event['text']
        response_url = event['response_url']
        slack_username = event['username']
        user_id = event['user_id']

        response = "unrecognized command, please try one of these:\n/lock list\n/lock add [username] [branch]\n/lock remove [username]\n/lock back [username]\n/lock register me [github username]\n/lock window open\n/lock window close\n(tip: you can use _me_ instead of your username)"

        if len(text) == 2 and text[0].lower() == 'adddeveloper':
            username = _resolve_username(text[1], slack_username, user_id)
            response = _add_developer(username, slack_username)

        elif len(text) == 1 and text[0].lower() == 'createrotation':
            _create_rotation()
            response = _list_full_active_rotation()

    except Exception as e:
        logger.error("Exception: %s" % e)
        response = unknown_error_message

    logger.info("Command response: %s" % response)
    headers = {'content-type': 'application/json'}
    payload = {'text': response}
    requests.post(response_url, data=json.dumps(payload), headers=headers)

def _validate(token):
    if (token != SLACK_TOKEN):
        raise Exception("Incorrect token")

def _resolve_username(command_username, requester_username, requester_user_id):
    if command_username.lower() == 'me':
        return "<@%s|%s>"%(requester_user_id,requester_username)
    else:
        return command_username

def _add_developer(username, requester_username):
    logger.info("Addition of developer %s invoked by %s" % (username, requester_username))
    table_name = 'developer'
    try:
        developer_sequence = _get_max_developer_sequence() + 1
        _insert_to_list(username, developer_sequence, table_name)
        return "Developer %s added in list with sequence number %i" % (username, developer_sequence)
    except botocore.exceptions.ClientError as e:
        return _process_exception_for_insert(e, username, table_name)
    except Exception as e:
        logger.error(e)
        return unknown_error_message

def _create_rotation():
    logger.info("Create rotation")

#     delete all rotation entries with start date greater or equal previous monday
    next_developer = _get_next_developer()
    logger.info("next developer: %s" % next_developer)
    developers = _get_developers()
    logger.info("developers: %s" % developers)
    next_monday = _get_next_monday_timestamp()
    logger.info("next monday: %s" % next_monday)
    for index in xrange(0, len(developers)):
        logger.info("index: %i" % index)
        developer1_index = int((index + next_developer['sequence'] - 1) % len(developers))
        logger.info("developer1_index: %s" % str(developer1_index))
        developer2_index = int((index + next_developer['sequence']) % len(developers))
        logger.info("developer2_index: %s" % str(developer2_index))
        developer1_name = developers[developer1_index]['username']
        logger.info("developer1_name: %s" % developer1_name)
        developer2_name = developers[developer2_index]['username']
        logger.info("developer2_name: %s" % developer2_name)
        _insert_to_rotation(developer1_name, developer2_name, next_monday)
        logger.info("INSERTION DONE")
        next_monday = _get_next_monday_timestamp(next_monday)
        logger.info("next monday: %s" % next_monday)

def _delete_rotation():
    logger.info("Delete rotation")

def _list_rotation():
    logger.info("List rotation")

def _get_previous_monday_timestamp():
    today = datetime.date.today()
    today_day_of_week = today.strftime("%w")
    days_to_subtract = (int(today_day_of_week) - 1) % 7
    previous_monday = today - datetime.timedelta(days=days_to_subtract)
    return int(time.mktime(previous_monday.timetuple()))

def _get_next_monday_timestamp(current_monday = None):
    if current_monday is None:
        current_day = datetime.date.today()
        current_day_of_week = current_day.strftime("%w")
        days_to_add = (8 - int(current_day_of_week)) % 7
        next_monday = current_day + datetime.timedelta(days=days_to_add)
    else:
        current_day = datetime.datetime.fromtimestamp(current_monday)
        next_monday = current_day + datetime.timedelta(days=7)

    logger.info("Next monday: %s" % next_monday)
    return int(time.mktime(next_monday.timetuple()))

def _get_table(table_name):
    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    return dynamodb.Table("%s-%s" %(table_name, STAGE))

def _get_max_developer_sequence():
    response = _get_table('developer').query(
        IndexName='active_sequence_index',
        KeyConditionExpression = Key('active').eq(1),
        Limit = 1,
        ScanIndexForward = False
    )
    if len(response['Items']) > 0:
        return response['Items'][0]['sequence']

    return 0

def _get_developer(developer_name):
    response = _get_table('developer').query(
        KeyConditionExpression = Key('username').eq(developer_name)
    )

    if len(response['Items']) > 0:
        return response['Items'][0]

    return None

def _get_developers():
    response = _get_table('developer').query(
        IndexName='active_sequence_index',
        KeyConditionExpression = Key('active').eq(1)
    )

    return response['Items']

def _get_first_developer_for_rotation():
    response = _get_table('developer').query(
        IndexName='active_sequence_index',
        KeyConditionExpression = Key('active').eq(1),
        Limit = 1
    )

    if len(response['Items']) > 0:
        return response['Items'][0]

    raise Exception("There are no developers in the rotation list")

def _get_next_developer():
    previous_monday_timestamp = _get_previous_monday_timestamp()
    table = _get_table('rotation')
    response = table.query(
        KeyConditionExpression = Key('type').eq('rotation') & Key('monday_timestamp').eq(previous_monday_timestamp),
        Limit = 1
    )

    if len(response['Items']) > 0:
        developer_name = response['Items'][0]['developer2']
        return _get_developer(developer_name)


    return _get_first_developer_for_rotation()

def _insert_to_list(username, sequence, table_name):
    table = _get_table(table_name)
    return table.put_item (
        Item = {
            'username': username,
            'sequence': sequence,
            'active' : 1
        },
        ConditionExpression = 'attribute_not_exists(username)'
    )

def _insert_to_rotation(developer1, developer2, timestamp):
    logger.info("Inserting to rotation: %s, %s, %s" % (developer1, developer2, timestamp))
    table = _get_table('rotation')
    return table.put_item (
        Item = {
            'type': 'rotation',
            'developer1': developer1,
            'developer2': developer2,
            'designer': 'TODO',
            'monday_timestamp' : timestamp
        },
        ConditionExpression = 'attribute_not_exists(monday_timestamp)'
    )

def _list_full_active_rotation():
    previous_monday_timestamp = _get_previous_monday_timestamp()
    response = _get_table('rotation').query(
        KeyConditionExpression = Key('type').eq('rotation') & Key('monday_timestamp').gte(previous_monday_timestamp),
    )

    rotation_message = 'Currently there is no rotation. Please run the command createRotation to create a new one.'
    if len(response['Items']) > 0:
        rotation_message = ''
        for item in response['Items']:
            date = datetime.datetime.fromtimestamp(item['monday_timestamp'])
            rotation_message += '%s: %s, %s\n' % (date, item['developer1'], item['developer2'])

    return rotation_message

def _process_exception_for_insert(e, username, group):
    if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
        return "User %s is already in the %s list" % (username, group)

    logger.error(e)
    return unknown_error_message