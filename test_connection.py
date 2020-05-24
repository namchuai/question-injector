from my_connector import SqlWorker

connector=SqlWorker('localhost', 'questioninjector', 'namh', '')
connector.connect()
connector.insert_part6('Test paragraph')
connector.dispose()
