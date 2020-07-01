import firebase_admin
import google.cloud
from firebase_admin import credentials, firestore,storage
import json
import xlrd
from PIL import Image
from pathlib import Path

class FirestorePush:
    def __init__(self):
        cred = credentials.Certificate('./ServiceAccountKey.json')
        self.app=firebase_admin.initialize_app(cred)
        self.store=firestore.client()

        self.loc=('learnenglish_data_new.xlsx')
        self.wb=xlrd.open_workbook(self.loc)

        self.bucket=storage.bucket('junes-7b38b.appspot.com')
        self.res1='./res1/'
        self.res2='./res2/'
        self.res3='./res3/'
        self.res4='./res4/'
    
    def insert_part_4(self):
        question_ref=self.store.collection('questions')
        sheet=self.wb.sheet_by_name('Part4')
        current_row=1
        total_row=sheet.nrows

        while (current_row<total_row):
            question_id = sheet.cell_value(current_row,1).strip()
            if len(question_id) == 0:
                question_id = question_ref.document().id

            question_index = int(sheet.cell_value(current_row,0))

            audio_path = "{}{}.mp3".format(self.res4, question_index)
            self.upload_audio(audio_path, question_id)

            self.insert_explaination(sheet.cell_value(current_row, 23), sheet.cell_value(current_row,24), question_id)

            image_path = "{}{}.jpg".format(self.res4, question_index)
            remote_image_path=self.upload_image(image_path, question_id)
            data = {}
            if remote_image_path is None:
                data = {
                    'a': question_id+'.mp3',
                    'cC': 0, # comment count
                    'eC': 1, # explaination count
                    'q': [
                        {
                            'c': sheet.cell_value(current_row, 8),
                            'q': sheet.cell_value(current_row, 2),
                            's': sheet.cell_value(current_row, 7)
                        },
                        {
                            'c': sheet.cell_value(current_row, 15),
                            'q': sheet.cell_value(current_row, 9),
                            's': sheet.cell_value(current_row, 14)
                        },
                        {
                            'c': sheet.cell_value(current_row, 22),
                            'q': sheet.cell_value(current_row, 16),
                            's': sheet.cell_value(current_row, 21)
                        },
                    ],
                    'qId': question_id,
                    't': 'toeic-p4'
                }
            else:
                data = {
                    'a': question_id+'.mp3',
                    'cC': 0, # comment count
                    'eC': 1, # explaination count
                    'i': remote_image_path,
                    'q': [
                        {
                            'c': sheet.cell_value(current_row, 8),
                            'q': sheet.cell_value(current_row, 2),
                            's': sheet.cell_value(current_row, 7)
                        },
                        {
                            'c': sheet.cell_value(current_row, 15),
                            'q': sheet.cell_value(current_row, 9),
                            's': sheet.cell_value(current_row, 14)
                        },
                        {
                            'c': sheet.cell_value(current_row, 22),
                            'q': sheet.cell_value(current_row, 16),
                            's': sheet.cell_value(current_row, 21)
                        },
                    ],
                    'qId': question_id,
                    't': 'toeic-p4'
                }
            question_ref.document(question_id).set(data)
            print('qId: '+question_id)
            current_row+=1
        print('Finished')

    def insert_part_3(self):
        question_ref=self.store.collection('questions')
        sheet=self.wb.sheet_by_name('Part3')
        current_row=1
        total_row=sheet.nrows

        while (current_row<total_row):
            question_id = sheet.cell_value(current_row,1).strip()
            if len(question_id) == 0:
                question_id = question_ref.document().id

            question_index = int(sheet.cell_value(current_row,0))

            audio_path = "{}{}.mp3".format(self.res3, question_index)
            self.upload_audio(audio_path, question_id)

            self.insert_explaination(sheet.cell_value(current_row, 23), sheet.cell_value(current_row,24), question_id)

            image_path = "{}{}.jpg".format(self.res3, question_index)
            remote_image_path=self.upload_image(image_path, question_id)
            data = {}
            if remote_image_path is None:
                data = {
                    'a': question_id+'.mp3',
                    'cC': 0, # comment count
                    'eC': 1, # explaination count
                    'q': [
                        {
                            'c': sheet.cell_value(current_row, 8),
                            'q': sheet.cell_value(current_row, 2),
                            's': sheet.cell_value(current_row, 7)
                        },
                        {
                            'c': sheet.cell_value(current_row, 15),
                            'q': sheet.cell_value(current_row, 9),
                            's': sheet.cell_value(current_row, 14)
                        },
                        {
                            'c': sheet.cell_value(current_row, 22),
                            'q': sheet.cell_value(current_row, 16),
                            's': sheet.cell_value(current_row, 21)
                        },
                    ],
                    'qId': question_id,
                    't': 'toeic-p3'
                }
            else:
                data = {
                    'a': question_id+'.mp3',
                    'cC': 0, # comment count
                    'eC': 1, # explaination count
                    'i': remote_image_path,
                    'q': [
                        {
                            'c': sheet.cell_value(current_row, 8),
                            'q': sheet.cell_value(current_row, 2),
                            's': sheet.cell_value(current_row, 7)
                        },
                        {
                            'c': sheet.cell_value(current_row, 15),
                            'q': sheet.cell_value(current_row, 9),
                            's': sheet.cell_value(current_row, 14)
                        },
                        {
                            'c': sheet.cell_value(current_row, 22),
                            'q': sheet.cell_value(current_row, 16),
                            's': sheet.cell_value(current_row, 21)
                        },
                    ],
                    'qId': question_id,
                    't': 'toeic-p3'
                }
            question_ref.document(question_id).set(data)
            print(question_id)
            current_row+=1
        print('Finished')

    def insert_part_2(self):
        question_ref=self.store.collection('questions')
        sheet=self.wb.sheet_by_name('Part2')
        current_row=251

        total_row=sheet.nrows
        while (current_row<total_row):
            question_id = sheet.cell_value(current_row,1).strip()
            if len(question_id) == 0:
                question_id = question_ref.document().id

            correct_answer = sheet.cell_value(current_row,2).strip()
            question_index = int(sheet.cell_value(current_row,0))
            audio_path = "{}{}.mp3".format(self.res2, question_index)
            self.upload_audio(audio_path, question_id)

            self.insert_explaination(sheet.cell_value(current_row, 3), sheet.cell_value(current_row,4), question_id)
            data = {
                'a': question_id+'.mp3',
                'cC': 0, # comment count
                'eC': 1, # explaination count
                'q': [
                    {
                        'c': correct_answer
                    }
                ],
                'qId': question_id,
                't': 'toeic-p2'
            }
            question_ref.document(question_id).set(data)
            print(question_id)
            current_row+=1
        print('Finished insert part 2')

    def insert_part_1(self):
        question_ref=self.store.collection('questions')
        sheet=self.wb.sheet_by_name('Part1')
        current_row=61
        total_row=sheet.nrows
        while (current_row<total_row):
            question_id = sheet.cell_value(current_row,1).strip()
            if len(question_id) == 0:
                question_id = question_ref.document().id

            correct_answer = sheet.cell_value(current_row,2).strip()
            question_index = int(sheet.cell_value(current_row,0))

            audio_path = "{}{}.mp3".format(self.res1, question_index)
            image_path = "{}{}.jpg".format(self.res1, question_index)

            self.upload_audio(audio_path, question_id)
            remote_image_path=self.upload_image(image_path, question_id)

            self.insert_explaination(sheet.cell_value(current_row, 3), sheet.cell_value(current_row,4), question_id)
            
            data = {
                'a': question_id+'.mp3',
                'cC': 0, # comment count
                'eC': 1, # explaination count
                'i': remote_image_path,
                'q': [
                    {
                        'c': correct_answer
                    }
                ],
                'qId': question_id,
                't': 'toeic-p1'
            }
            
            question_ref.document(question_id).set(data)
            print(question_id)
            current_row+=1
        print('Finished insert part 1')

    def upload_image(self, image_path, question_id):
        my_file = Path(image_path)
        if my_file.is_file():
            img = Image.open(image_path)
            width, height = img.size

            remote_image_name='%s_%s_%s.jpg'%(question_id, width, height)
            imageBlob = self.bucket.blob('question_resources/'+remote_image_name)
            imageBlob.upload_from_filename(image_path, content_type='image/jpg')

            return remote_image_name
        else:
            return None
    
    def upload_audio(self, audio_local_path, question_id):
        audio_blob=self.bucket.blob('question_resources/%s.mp3'%question_id)
        audio_blob.upload_from_filename(audio_local_path, content_type='audio/mpeg')

    def insert_explaination(self, eId, explaination, question_id):
        explaination_ref=self.store.collection('explainations')
        if len(eId) == 0:
            eId=explaination_ref.document().id
        data = {
            'eId': eId,
            'qId': question_id,
            't': 'practice', # type for practice question
            'e': explaination
        }
        explaination_ref.document(eId).set(data)
        print('eId: {}'.format(eId))

    def add_explaination_p5(self):
        explaination_ref=self.store.collection('explainations')
        question_ref=self.store.collection('questions')
        sheet=self.wb.sheet_by_name('Part5')
        current_row=1
        total_row=sheet.nrows
        while (current_row<total_row):
            qId = sheet.cell_value(current_row, 1)
            if len(qId) > 0:
                explaination=sheet.cell_value(current_row, 10)
                if len(explaination) > 0:
                    eId=sheet.cell_value(current_row,9)
                    if len(eId) == 0:
                        eId=explaination_ref.document().id
                    # print('Row num: {}'.format(current_row))
                    # print("{}-{}".format(eId, qId))
                    print("{}".format(eId))
                    data = {
                        'eId': eId,
                        'qId': qId,
                        't': 'practice', # type for practice question
                        'e': explaination
                    }
                    explaination_ref.document(eId).set(data)
                    question_ref.document(qId).update({
                        'eC': 1
                    })
            current_row+=1
        print('Finished!')

    def add_part5(self):
        question_ref = self.store.collection('questions')
        loc=('learnenglish_data.xlsx')
        wb=xlrd.open_workbook(loc)
        sheet=wb.sheet_by_name('Part5')

        current_row=1
        total_row=sheet.nrows

        while (current_row < total_row):
            qId = sheet.cell_value(current_row, 1)
            if len(qId) == 0:
                qId = question_ref.document().id
            data = {
                'qId': qId, # currently using this field to random query
                't': 'toeic-p5',
                'cC': 0, # comment count
                'eC': 0, # explaination count
                'q': [
                    {
                        'q': sheet.cell_value(current_row, 2).strip(),
                        's': sheet.cell_value(current_row, 8).strip(),
                        'c': sheet.cell_value(current_row, 7).strip()
                    }
                ]
            }
            question_ref.document(qId).set(data)
            print(qId)
            current_row+=1

        # record_id=sheet.cell_value(1,1)
        # paragraph=sheet.cell_value(1,2)

    def add_part6(self, record_id, paragraph, questions):
        question_ref = self.store.collection('questions')
        qId = record_id
        if len(qId) == 0:
            qId = question_ref.document().id
        
        data = {
            'qId': qId,
            't': 'toeic-p6',
            'cC': 0,
            'eC': 0,
            'p' : paragraph,
            'q': questions
        }
        question_ref.document(qId).set(data)
        return qId

    def insert_part7(self):
        question_ref = self.store.collection('questions')
        loc=('learnenglish_data_new2017.xlsx')
        wb=xlrd.open_workbook(loc)
        sheet=wb.sheet_by_name('Part7')

        current_row = 1

        question_row_ids = [3, 10, 17, 24, 31]

        total_row=sheet.nrows
        while (current_row<total_row):
            paragraph = sheet.cell_value(current_row, 2)
            questions_data = []
            for id in question_row_ids:
                question=sheet.cell_value(current_row, id)
                if len(question) == 0:
                    break
                else:
                    question_data = {
                        'q': question,
                        's': sheet.cell_value(current_row, id+5),
                        'c': sheet.cell_value(current_row, id+6)
                    }
                    questions_data.append(question_data)
            
            qId = sheet.cell_value(current_row, 1)
            if len(qId) == 0:
                qId = question_ref.document().id

            data = {
                'qId': qId,
                't': 'toeic-p7',
                'cC': 0,
                'eC': 0,
                'p' : paragraph,
                'q': questions_data
            }
            question_ref.document(qId).set(data)
            print(qId)
            current_row+=1

    def read_excel(self):
        loc=('learnenglish_data.xlsx')
        wb=xlrd.open_workbook(loc)
        sheet=wb.sheet_by_name('Part6')
        
        current_row = 1
        total_row = sheet.nrows
        while (current_row < total_row):
            # print('Processing row {}'.format(current_row))
            # wb_write=Workbook()
            # p6_sheet=wb_write.sheet_by_name('Part6')

            record_id=sheet.cell_value(current_row,1)
            paragraph=sheet.cell_value(current_row,2)

            q0= {
                's': sheet.cell_value(current_row,7),
                'c':sheet.cell_value(current_row,8)
            }
            q1= {
                's': sheet.cell_value(current_row,13),
                'c':sheet.cell_value(current_row,14)
            }
            q2= {
                's': sheet.cell_value(current_row,19),
                'c':sheet.cell_value(current_row,20)
            }
            q3= {
                's': sheet.cell_value(current_row,25),
                'c':sheet.cell_value(current_row,26)
            }
            questions=[q0,q1,q2,q3]
            qId = self.add_part6(record_id, paragraph, questions)
            print(qId)
            # p6_sheet.write(current_row, 1, qId)
            current_row+=1
        print('Finished!')

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
    f.insert_part7()
    # f.insert_part_2()
    # f.add_explaination_p5()
    # f.test_upload_p6()
    #f.my_push()
