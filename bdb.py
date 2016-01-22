import datetime


schema = dict(
    version=1,
    tables=dict(
        user=dict(
            _id=int,
            first_name=str,
            last_name=str,
        )
    )
)


class View(object):
    pass


class Stream(object):
    def __init__(self, path):
        self.path = path

    def push_entry(self, entry):
        data = dict(
            _timestamp=datetime.datetime.now(),
            _schema_version=schema['version'],
        )
        data.update(entry)

        table = entry['_table']
        table_schema = schema['tables'][table]

        if table not in schema['tables'].keys():
            raise Exception('Table {0} not defined in schema.'.format(table))

        for key in entry.keys():
            if key != '_table':
                if key not in table_schema.keys():
                    raise Exception('Key {0} not defined in schema.'.format(key))
                if type(entry[key]) != table_schema[key]:
                    raise Exception('Invalid type.')

        with open(self.path, 'a') as f:
            f.write(str(data) + '\n')

stream = Stream('db.txt')
entry = dict(
    _id=1,
    _table='user',
    first_name='Jason',
)
stream.push_entry(entry)
