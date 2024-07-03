from helpers import constants

"""
Config dictionary to extract destination table name to load data for externalIdPipeline.py pipeline 
and source table name for bigqueryToPubsub.py
"""

bigquery_table_config = {
    "147": {
        "destination_registration_table": "brazil_registration_data_delta",
        "destination_clickstream_table": "brazil_clickstream_data_delta",
        "destination_clt_table": "brazil_clt_data_delta",
        "registration_data": {
            "source_table": "brazil_registration_data_final",
        },
        "clickstream_data": {
            "source_table": "brazil_clickstream_data_final",
        },
        "clt_data": {
            "source_table": "brazil_clt_data_final",
        }
    },
    "550": {
    },
    "138": {
        "destination_registration_table": "can_growing_families_registration_data",
        "destination_clickstream_table": "",
        "destination_clt_table": "",
        "registration_data": {
            "source_table": "can_growing_families_registration_data_final",
        },
        "clickstream_data": {
            "source_table": "",
        },
        "clt_data": {
            "source_table": "",
        }
    },
    "146": {
        "destination_registration_table": "lat_rola_registration_data",
        "destination_clickstream_table": "",
        "destination_clt_table": "",
        "registration_data": {
            "source_table": "lat_rola_registration_data_final",
        },
        "clickstream_data": {
            "source_table": "",
        },
        "clt_data": {
            "source_table": "",
        }
    },
    "115": {
        "destination_registration_table": "usa_growing_families_registration_data",
        "registration_data": {
            "source_table": "usa_growing_families_registration_data_final",
        }
    },
    "401": {
        "destination_registration_table": "usa_dental_care_registration_data",
        "registration_data": {
            "source_table": "usa_dental_care_registration_data_final",
        },"pg_id_downstream_update_table": "updation_downstream_ids_401_usa_dental_care"
    },
    "290": {
        "destination_registration_table": "esp_growing_families_registration_data",
        "registration_data": {
            "source_table": "esp_growing_families_registration_data_final",
        },
        "pg_id_downstream_update_table": "esp_ghh_290_datalake_remediation_march21"
    },
    "300": {
        "destination_registration_table": "prt_growing_families_registration_data",
        "registration_data": {
            "source_table": "prt_growing_families_registration_data_final",
        }, "pg_id_downstream_update_table": "prt_datalake_userids_remediation_mar19"
    },
    "2": {
        "destination_registration_table": "jpn_growing_families_registration_data",
        "front_load_validation_failure_table": "jpn_growing_families_front_load_validation_failure",
        "registration_data": {
            "source_table": "jpn_growing_families_registration_data_final",
        }
    },
    "86": {
        "destination_registration_table": "idn_corporate_registration_data",
        "front_load_validation_failure_table": "idn_corporate_front_load_validation_failure",
        "registration_data": {
            "source_table": "idn_corporate_registration_data_final",
        }
    },
    "350": {
        "destination_registration_table": "ita_braun_registration_data",
        "front_load_validation_failure_table": "ita__front_load_validation_failure",
        "registration_data": {
            "source_table": "ita_braun_registration_data_final",
        }
    },
    "378": {
        "destination_registration_table": "egy_pampers_registration_data",
        "front_load_validation_failure_table": "egy_pampers_front_load_validation_failure",
        "registration_data": {
            "source_table": "egy_pampers_registration_data_final",
        }
    },
    "473": {
        "destination_registration_table": "aus_always_discreet_registration_data",
        "front_load_validation_failure_table": "aus_always_discreet_front_load_validation_failure",
        "registration_data": {
            "source_table": "aus_always_discreet_registration_data_final",
        }
    },
    "16": {
        "destination_registration_table": "jpn_whisper_registration_data",
        "front_load_validation_failure_table": "jpn_whisper_front_load_validation_failure",
        "registration_data": {
            "source_table": "jpn_whisper_registration_data_final",
        }
    },
    "543": {
        "destination_registration_table": "esp_beautycode_registration_data",
        "front_load_validation_failure_table": "esp_beautycode_front_load_validation_failure",
        "registration_data": {
            "source_table": "esp_beautycode_registration_data_final",
        }
    },
    "295": {
        "destination_registration_table": "che_growing_families_registration_data",
        "front_load_validation_failure_table": "che_growing_families_front_load_validation_failure",
        "registration_data": {
            "source_table": "che_growing_families_registration_data_final",
        },"pg_id_downstream_update_table": "updation_downstream_ids_295_che_growingfamilies"
    },
    "24": {
        "destination_registration_table": "phl_corporate_registration_data",
        "front_load_validation_failure_table": "phl_corporate_front_load_validation_failure",
        "registration_data": {
            "source_table": "phl_corporate_registration_data_final",
        }
    },
    "44": {
        "destination_registration_table": "phl_pampers_registration_data",
        "front_load_validation_failure_table": "phl_pampers_front_load_validation_failure",
        "registration_data": {
            "source_table": "phl_pampers_registration_data_final",
        }
    },
    "293": {
        "destination_registration_table": "deu_growing_families_registration_data",
        "front_load_validation_failure_table": "deu_growing_families_front_load_validation_failure",
        "registration_data": {
            "source_table": "deu_growing_families_registration_data_final",
        },"pg_id_downstream_update_table": "updation_downstream_ids_293_deu_growingfamilies"
    },
    "363": {
        "destination_registration_table": "arb_growing_families_everyday_me_arabia_registration_data",
        "front_load_validation_failure_table": "arb_growing_families_everyday_me_arabia_front_load_validation_failure",
        "registration_data": {
            "source_table": "arb_growing_families_everyday_me_arabia_registration_data_final",
        }
    },
    "299": {
        "destination_registration_table": "irl_growing_families_registration_data",
        "front_load_validation_failure_table": "irl_growing_families_front_load_validation_failure",
        "registration_data": {
            "source_table": "irl_growing_families_registration_data_final",
        },"pg_id_downstream_update_table": "updation_downstream_ids_299_irl_growingfamilies"
    },
    "178": {
        "destination_registration_table": "usa_old_spice_registration_data",
        "front_load_validation_failure_table": "usa_old_spice_front_load_validation_failure",
        "registration_data": {
            "source_table": "usa_old_spice_registration_data_final",
        }
    },
    "288": {
        "destination_registration_table": "gbr_growing_families_registration_data",
        "front_load_validation_failure_table": "gbr_growing_families_front_load_validation_failure",
        "registration_data": {
            "source_table": "gbr_growing_families_registration_data_final",
        },"pg_id_downstream_update_table": "updation_downstream_ids_288_gbr_growingfamilies"
    },
    "289": {
        "destination_registration_table": "ita_growing_families_registration_data",
        "front_load_validation_failure_table": "ita_growing_families_front_load_validation_failure",
        "registration_data": {
            "source_table": "ita_growing_families_registration_data_final",
        }
    },
    "417": {
        "destination_registration_table": "gbr_oral_b_registration_data",
        "front_load_validation_failure_table": "gbr_oral_b_front_load_validation_failure",
        "registration_data": {
            "source_table": "gbr_oral_b_registration_data_final",
        }
    },
    "416": {
        "destination_registration_table": "deu_oral_b_registration_data",
        "front_load_validation_failure_table": "deu_oral_b_front_load_validation_failure",
        "registration_data": {
            "source_table": "deu_oral_b_registration_data_final",
        }
    },
    "412": {
        "destination_registration_table": "esp_oral_b_registration_data",
        "front_load_validation_failure_table": "esp_oral_b_front_load_validation_failure",
        "registration_data": {
            "source_table": "esp_oral_b_registration_data_final",
        }
    },
    "414": {
        "destination_registration_table": "nld_oral_b_registration_data",
        "front_load_validation_failure_table": "nld_oral_b_front_load_validation_failure",
        "registration_data": {
            "source_table": "nld_oral_b_registration_data_final",
        }
    },

    "524": {
        "destination_registration_table": "bel_oral_b_registration_data",
        "front_load_validation_failure_table": "bel_oral_b_front_load_validation_failure",
        "registration_data": {
            "source_table": "bel_oral_b_registration_data_final",
        }
    },
    "411": {
        "destination_registration_table": "pol_oral_b_registration_data",
        "front_load_validation_failure_table": "pol_oral_b_front_load_validation_failure",
        "registration_data": {
            "source_table": "pol_oral_b_registration_data_final",
        }
    },
    "410": {
        "destination_registration_table": "swe_oral_b_registration_data",
        "front_load_validation_failure_table": "swe_oral_b_front_load_validation_failure",
        "registration_data": {
            "source_table": "swe_oral_b_registration_data_final",
        }
    },
    "517": {
        "destination_registration_table": "fin_oral_b_registration_data",
        "front_load_validation_failure_table": "fin_oral_b_front_load_validation_failure",
        "registration_data": {
            "source_table": "fin_oral_b_registration_data_final",
        }
    },
    "39": {
        "destination_registration_table": "vnm_corporate_registration_data",
        "front_load_validation_failure_table": "vnm_corporate_front_load_validation_failure",
        "registration_data": {
            "source_table": "vnm_corporate_registration_data_final",
        }
    },
    "413": {
        "destination_registration_table": "ita_oral_b_registration_data",
        "front_load_validation_failure_table": "ita_oral_b_front_load_validation_failure",
        "registration_data": {
            "source_table": "ita_oral_b_registration_data_final",
        }
    },
	"516": {
        "destination_registration_table": "dnk_oral_b_registration_data",
        "front_load_validation_failure_table": "dnk_oral_b_front_load_validation_failure",
        "registration_data": {
            "source_table": "dnk_oral_b_registration_data_final",
        }
    },
    "127": {
        "destination_registration_table": "usa_olay_registration_data",
        "front_load_validation_failure_table": "usa_olay_front_load_validation_failure",
        "registration_data": {
        "source_table": "usa_olay_registration_data_final",
        },
        "pg_id_downstream_update_table": "usa_olay_updation_ids_downstreams"
    },
    "507": {
        "destination_registration_table": "fra_braun_registration_data",
        "destination_registration_table_v2": "fra_braun_registration_data_v2",
        "front_load_validation_failure_table": "fra_braun_front_load_validation_failure",
        "registration_data": {
            "source_table": "fra_braun_registration_data",
        }
    },
    "472": {
        "destination_registration_table": "fra_jolly_registration_data",
        "front_load_validation_failure_table": "fra_jolly_front_load_validation_failure",
        "registration_data": {
            "source_table": "fra_jolly_registration_data",
        }
    },
    "518": {
        "destination_registration_table": "esp_braun_registration_data",
        "front_load_validation_failure_table": "esp_braun_front_load_validation_failure",
        "registration_data": {
            "source_table": "esp_braun_registration_data",
        }
    },
    "424": {
        "destination_registration_table": "nld_gillette_registration_data",
        "front_load_validation_failure_table": "nld_gillette_front_load_validation_failure",
        "registration_data": {
            "source_table": "nld_gillette_registration_data",
        }
    },
	"59": {
        "destination_registration_table": "tw_living_artist_registration_data",
        "destination_registration_table_v2": "tw_living_artist_registration_data_v2",
        "front_load_validation_failure_table": "tw_living_artist_front_load_validation_failure",
        "registration_data": {
            "source_table": "tw_living_artist_registration_data",
        }
    },
	"353": {
        "destination_registration_table": "ita_gillette_registration_data",
        "front_load_validation_failure_table": "ita_gillette_front_load_validation_failure",
        "registration_data": {
            "source_table": "ita_gillette_registration_data",
        }
    },
	"308": {
        "destination_registration_table": "esp_gillette_registration_data",
        "front_load_validation_failure_table": "esp_gillette_front_load_validation_failure",
        "registration_data": {
            "source_table": "esp_gillette_registration_data",
        }
    },
    "119": {
        "destination_registration_table": "usa_gillette_v2_registration_data",
        "front_load_validation_failure_table": "usa_gillette_v2_front_load_validation_failure",
        "registration_data": {
            "source_table": "usa_gillette_v2_registration_data_final",
        }
    },
    "405": {
        "destination_registration_table": "can_dental_care_registration_data",
        "front_load_validation_failure_table": "can_dental_care_front_load_validation_failure",
        "registration_data": {
            "source_table": "can_dental_care_registration_data_final",
        }
    },
    "292": {
        "destination_registration_table": "grc_ghh_registration_data",
        "destination_registration_table_v2": "grc_ghh_registration_data_v2",
        "front_load_validation_failure_table": "grc_ghh_front_load_validation_failure",
        "registration_data": {
            "source_table": "grc_ghh_registration_data_final",
        }
    },
    "209": {
        "destination_registration_table": "tur_being_girl_registration_data",
        "destination_registration_table_v2": "tur_being_girl_registration_data_v2",
        "front_load_validation_failure_table": "tur_being_girl_front_load_validation_failure",
        "registration_data": {
            "source_table": "tur_being_girl_registration_data_final",
        }
    },
    "249": {
         "destination_registration_table": "pol_growing_families_registration_data",
         "destination_registration_table_v2": "pol_growing_families_registration_data_v2",
         "front_load_validation_failure_table": "pol_growing_families_front_load_validation_failure",
         "registration_data": {
            "source_table": "pol_growing_families_registration_data_final",
        }
    },
    "430": {
         "destination_registration_table": "usa_braun_registration_data",
         "destination_registration_table_v2": "usa_braun_registration_data_v2",
         "front_load_validation_failure_table": "usa_braun_front_load_validation_failure",
         "registration_data": {
            "source_table": "usa_braun_registration_data_final",
        }
    },
    "380": {
         "destination_registration_table": "afr_pampers_registration_data",
         "destination_registration_table_v2": "afr_pampers_registration_data_v2",
         "front_load_validation_failure_table": "afr_pampers_front_load_validation_failure",
         "registration_data": {
            "source_table": "afr_pampers_registration_data_final",
        }
    },
    "384": {
         "destination_registration_table": "bgr_corporate_registration_data",
         "destination_registration_table_v2": "bgr_corporate_registration_data_v2",
         "front_load_validation_failure_table": "bgr_corporate_front_load_validation_failure",
         "registration_data": {
            "source_table": "bgr_corporate_registration_data_final",
        }
    },
    "173": {
        "destination_registration_table": "usa_oral_care_registration_data",
        "destination_registration_table_v2": "usa_oral_care_registration_data_v2",
        "front_load_validation_failure_table": "usa_oral_care_front_load_validation_failure",
        "registration_data": {
            "source_table": "usa_oral_care_registration_data_final",
        }
    },
    "415": {
        "destination_registration_table": "fra_oralB_registration_data",
        "destination_registration_table_v2": "fra_oralB_registration_data_v2",
        "front_load_validation_failure_table": "fra_oralB_front_load_validation_failure",
        "registration_data": {
            "source_table": "fra_oralB_registration_data_final",
        }
    },
    "494": {
        "destination_registration_table": "usa_always_registration_data",
        "destination_registration_table_v2": "usa_always_registration_data_v2",
        "front_load_validation_failure_table": "usa_always_front_load_validation_failure",
        "registration_data": {
            "source_table": "usa_always_registration_data_final",
        }
    }
}

# dataset/Input Folder and pubsub topic name for each pipeline, no need to change

datasets_pubsub_config = {
    "registration_data": {
        "dataset": constants.REGISTRATION_BIGQUERY_DATASET,
        "pubsub_topic": constants.REGISTRATION_PUBSUB_TOPIC,
        "input_gcs_folder": 'externalid-output-registration'
    },
    "clickstream_data": {
        "dataset": constants.CLICKSTREAM_BIGQUERY_DATASET,
        "pubsub_topic": constants.CLK_PUBSUB_TOPIC,
        "input_gcs_folder": 'externalid-output-clickstream'
    },
    "clt_data": {
        "dataset": constants.CLT_BIGQUERY_DATASET,
        "pubsub_topic": constants.CLT_PUBSUB_TOPIC,
        "input_gcs_folder": 'externalid-output-clt'
    },
    "other_events": {
        "dataset": "",
        "pubsub_topic": constants.OTHER_PUBSUB_TOPIC,
        "input_gcs_folder": 'externalid-output-other-events'
    }
}

pg_id_update_table_config = {
    '350': {
        'pg_id_update': ['test_lookup1', 'test_lookup1'],
        'trace_id_update': 'staging_traces_mapping_test',
        'profile_consents_update': 'consents_mapping_test'
    }
}