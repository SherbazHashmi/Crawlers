from DBManager import DBManager

db_path = '../crawler.db'
schema_path = '../migrations/table_queries.json'
db_man = DBManager(db_path, schema_path)
entry = {
    'data': [
        {
            'fieldName': 'name',
            'value': 'sherbaz'
        }
    ]
}
db_man.add_entry('files', entry)
