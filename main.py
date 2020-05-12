import firebase_admin
import google.cloud
from firebase_admin import credentials, firestore
import json

class FirestorePush:
    def __init__(self):
        cred = credentials.Certificate('./ServiceAccountKey.json')
        self.app=firebase_admin.initialize_app(cred)
        self.store=firestore.client()

        with open('./data-p5.json','r') as f:
            self.data = json.load(f)

    def my_push(self):
        question_ref=self.store.collection('questions')
        total_record_cnt=len(self.data)

        curr_record_idx=0
        batch=self.store.batch()
        for record in self.data:
            if curr_record_idx%500==0:
                if curr_record_idx>0:
                    print('Commiting after 500 records..')
                    batch.commit()
                batch=self.store.batch()
            record_ref_id=''
            if 'qId' in record:
                record_ref_id=record['qId']
            else:
                record_ref_id=question_ref.document().id
            print(record_ref_id)
            batch.set(question_ref.document(record_ref_id), {
                'qId': record_ref_id, # currently using this field to random query
                't': 'toeic-p5',
                'cC': 0, # comment count
                'eC': 0, # explaination count
                'q': [
                    {
                        'q': record['q'].strip(),
                        's': record['s'].strip(),
                        'c': record['c'].strip()
                    }
                ]
            })
            curr_record_idx+=1
        print('Commit before reach 500 records..')
        batch.commit()
        print('Finished!')

if __name__ == '__main__':
    f = FirestorePush()
    f.my_push()

