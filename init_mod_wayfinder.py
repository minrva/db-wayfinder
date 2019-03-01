# pylint: disable=invalid-name
"""Digests mod-wayfinder json data."""
import csv, json, os, uuid, dataset


# directory
DATA_DIR = 'data'
SHELVES_CSV_FILE = 'shelves.csv'
SHELVES_JSON_FILE = 'shelves.json'

# postgres
PG_USER = 'folio_admin'
PG_PASSWORD = 'folio_admin'
PG_NETLOC = '10.0.2.15'
PG_PORT = '5432'
PG_DBNAME = 'okapi_modules'
PG_URL = ("postgresql://" + PG_USER + ":" + PG_PASSWORD +
          '@' + PG_NETLOC + ':' + PG_PORT + '/' + PG_DBNAME)

#  wayfinder schema
WF_SCHEMA = 'diku_mod_wayfinder'
WF_SHELVES_TBL = 'shelves'


def load_csv(fpath):
    """Loads a CSV file."""
    d = []
    with open(fpath, encoding='utf-8-sig') as fs:
        csvReader = csv.DictReader(fs)
        for csvRow in csvReader:
            d.append(csvRow)
    print("Loaded {0} objects from {1}...".format(len(d), fpath))
    return d


def load_json(fpath):
    """Loads a JSON file."""
    with open(fpath) as fs:
        d = json.load(fs)
    print("Loaded {0} objects from {1}...".format(len(d), fpath))
    return d


def load_table(tbl_name, schema_name):
    """Loads a postgres table."""
    rows = []
    with dataset.Database(url=PG_URL, schema=schema_name) as db:
        tbl = db[tbl_name]
        print("Loaded {0} rows from {1}.{2}...".format(
            len(tbl), schema_name, tbl_name))
        for row in tbl:
            rows.append(row)
    db.executable.close()
    db = None
    return rows


def csv_to_json(cpath, jpath):
    """Transforms CSV file to JSON file."""
    print("Transforming {0} to {1}...".format(cpath, jpath))
    shelves_csv = load_csv(cpath)
    with open(jpath, "w") as json_file:
        json_file.write(json.dumps(shelves_csv, indent = 4))


def create_table(rows, tbl_name, schema_name, clear=True):
    """Creates a postgres table."""
    print("Saving {0} rows to {1}.{2}...".format(
        len(rows), schema_name, tbl_name))
    with dataset.Database(url=PG_URL, schema=schema_name) as db:
        table = db[tbl_name]
        if clear:
            table.delete()
        table.insert_many(rows)
    db.executable.close()
    db = None


def create_shelf_row(data):
    """Creates a shelf row."""
    new_obj = {}
    new_obj['id'] = str(uuid.uuid4())
    new_obj['label'] = data['label']
    new_obj['lowerBound'] = data['lowerBound']
    new_obj['upperBound'] = data['upperBound']
    new_obj['mapTitle'] = data['mapTitle']
    new_obj['mapUri'] = data['mapUri']
    new_obj['x'] = data['x']
    new_obj['y'] = data['y']
    return dict(jsonb=new_obj)


if __name__ == '__main__':

    # transformed csv to json
    # csv_path = os.path.join(DATA_DIR, SHELVES_CSV_FILE)
    # json_path = os.path.join(DATA_DIR, SHELVES_JSON_FILE)
    # csv_to_json(csv_path, json_path)
    
    # load shelves
    print('Loading data...')    
    shelves_path = os.path.join(DATA_DIR, SHELVES_JSON_FILE)
    shelves_json = load_json(shelves_path)

    # transform data to database rows
    print('Transforming data to database rows...')
    shelf_rows = []
    for shelf_json in shelves_json:
         shelf_row = create_shelf_row(shelf_json)
         shelf_rows.append(shelf_row)

    # create database tables
    print('Creating database tables...')
    create_table(shelf_rows, WF_SHELVES_TBL, WF_SCHEMA)

    print('Complete...')
