from helpers import constants

dlq_config = {
    'registration_dlq':
        {'table_name': 'registration_dlq', 'pubsub_topic': constants.REGISTRATION_PUBSUB_TOPIC},
    'registration_pillar_dlq':
        {'table_name': 'cdp2_registrations_dlq', 'pubsub_topic': constants.REGISTRATION_PILLAR_PUBSUB_TOPIC},
    'clickstream_dlq':
        {'table_name': 'clickstream_dlq', 'pubsub_topic': constants.CLK_PUBSUB_TOPIC},
    'clt_dlq':
        {'table_name': 'clt_events_dlq', 'pubsub_topic': constants.CLT_PUBSUB_TOPIC},
    'other_events_dlq':
        {'table_name': 'other_events_dlq', 'pubsub_topic': constants.OTHER_PUBSUB_TOPIC},
}