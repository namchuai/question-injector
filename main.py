import firebase_admin
import google.cloud
from firebase_admin import credentials, firestore
import json

class FirestorePush:
    def __init__(self):
        cred = credentials.Certificate('./ServiceAccountKey.json')
        self.app=firebase_admin.initialize_app(cred)
        self.store=firestore.client()
        self.userId='NGO8RIbS2DhqVOiMolfwTX0r0qt2'

        with open('./data.txt','r') as f:
            self.data = json.load(f)

    def push(self):
        question_ref=self.store.collection('questions')
        total=len(self.data)
        idx=0

        batch=self.store.batch()
        for record in self.data:
            if idx % 500 ==0:
                if idx >0:
                    print('Committing..')
                    batch.commit()

                # start a new batch for the next iteration
                batch=self.store.batch()
            idx+=1
            print(str(idx) + str('/')+str(total)+": "+str(record['q']))
            record_ref=question_ref.document()

            batch.set(record_ref, {
                'qId': record_ref.id,
                'cC': 0,
                't': 'toeic-p5',
                'e': record['e'].strip(),
                'q': [
                    {
                        'q': record['q'].strip(),
                        's': record['s'].strip(),
                        'c': record['c'].strip()
                    }
                ]
            })

        # include current record in batch
        if idx %500 != 0:
            print('Committing..')
            batch.commit()

if __name__ == '__main__':
    f = FirestorePush()
    f.push()

