import firebase_admin
import google.cloud
from firebase_admin import credentials, firestore
import json
import xlrd

class FirestorePush:
    def __init__(self):
        cred = credentials.Certificate('./ServiceAccountKey.json')
        self.app=firebase_admin.initialize_app(cred)
        self.store=firestore.client()

        self.loc=('learnenglish_data.xlsx')
        self.wb=xlrd.open_workbook(self.loc)

        # with open('./data-p5.json','r') as f:
            # self.data = json.load(f)

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
    f.add_explaination_p5()
    # f.read_excel()
    # f.test_upload_p6()
    #f.my_push()

