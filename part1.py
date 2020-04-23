import firebase_admin
import google.cloud
from firebase_admin import credentials, firestore, storage
from PIL import Image
import json

class FirestorePush:
    def __init__(self):
        cred = credentials.Certificate('./ServiceAccountKey.json')
        self.app=firebase_admin.initialize_app(cred)
        self.store=firestore.client()
        self.bucket=storage.bucket('junes-7b38b.appspot.com')
        self.base_path='./data/part1/'

        with open('%sdata.txt'%self.base_path,'r') as f:
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
            print(str(idx) + str('/')+str(total)+": "+str(record['id']))
            record_ref=question_ref.document()
            
            # image
            image_name = "%s.png"%record['id']
            img = Image.open('%s%s'%(self.base_path, image_name))
            width, height = img.size

            # processing the image
            img=img.resize((int(width/2),int(height/2)), Image.ANTIALIAS)
            img.save('%s%s'%(self.base_path, image_name), optimize=True, quality=10)
            width, height = img.size

            # audio
            audio_file = '%s.mp3'%(record['id'])
            # bucket.blob('%s.mp3'%record_ref.id)
            print(audio_file)

            print('Uploading image..')
            imageBlob = self.bucket.blob('question_resources/%s_%s_%s.png'%(record_ref.id, width, height))
            imageBlob.upload_from_filename('%s%s'%(self.base_path, image_name), content_type='image/jpg')

            print('Image uploaded %s'%record_ref.id)

            print('Uploading audio')
            audio_blob=self.bucket.blob('question_resources/%s.mp3'%record_ref.id)
            audio_blob.upload_from_filename('%s%s'%(self.base_path, audio_file))

            upload_audio_name='%s.mp3'%record_ref.id
            upload_image_name='%s_%s_%s.png'%(record_ref.id, width, height)

            batch.set(record_ref, {
                'qId': record_ref.id,
                'cC': 0,
                't': 'toeic-p1',
                'e': record['e'].strip(),
                'a': upload_audio_name,
                'i': upload_image_name,
                'q': [
                    {
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

