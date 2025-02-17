from dotenv import load_dotenv

load_dotenv()


if __name__ == '__main__':
    match 1:
        case 1: # upload subject
            from upload_study.index import handler
            handler({'fileName': 'D:/tmp/debug/test.csv', 'user': 'py_test_user'}, None)
        case 2: # upload subject generate response
            from generate_subject_response.index import handler
            handler({"studyIds": [405], 'user': 'py_test_user'}, None)
        case 3: # make draft generate response
            from generate_subject_response.index import handler
            handler({"studyId": 448, "draftId": 1, 'user': 'py_test_user'}, None)
        case 4: # api generate response
            from generate_subject_response.index import handler
            handler({'entityId': 378, 'entityType': 'SURVEY', 'user': 'py_test_user'}, None)
        case 5: # subject Period status job
            from participant_phase.index import handler
            handler({'user': 'py_test_user'}, None)
        case 6: # study notify synchronization
            from study_data_sync.index import handler
            handler({'user': 'py_test_user', 'type': 'STUDY_SYNC_NOTICE', 'studyId': 438}, None)




