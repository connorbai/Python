from common.common_logger import logger
from database.connect import connect_to_db
from generate_subject_response.gen_entity import generate_entity_response
from generate_subject_response.gen_individual import generate_individual_response
from generate_subject_response.gen_study import generate_study_response
from generate_subject_response.notif_individual import notif_individual


def handler(event, context):
    logger.info("------generate subject response start------")
    logger.info("event: %s", str(event))

    study_ids = event.get('studyIds', None)
    study_id = event.get('studyId', None)
    draft_id = event.get('draftId', None)
    entity_id = event.get('entityId', None)
    entity_type = event.get('entityType', None)
    usr = event.get('user', None)
    if (not study_ids and not study_id) and (not entity_id and not entity_type):
        return logger.error(f'parameter error: {study_ids=}, {study_id=}, {entity_id=}, {entity_type=}')

    is_make_draft = False
    if draft_id is not None and draft_id != study_id:
        is_make_draft = True

    with connect_to_db() as (pubdb, trxdb):
        # activate study logic
        if study_id is not None:
            generate_study_response(pubdb, trxdb, study_id, usr)
            if is_make_draft:
                generate_individual_response(pubdb, trxdb, study_id, usr)
                notif_individual(pubdb, trxdb, study_id, usr)
        # upload csv logic
        elif study_ids and len(study_ids) > 0:
            study_list = list(set(study_ids))
            for study_id in study_list:
                generate_study_response(pubdb, trxdb, study_id, usr)
        # adhoc api logic
        elif entity_id and entity_type:
            generate_entity_response(pubdb, trxdb, entity_id, entity_type, usr)
