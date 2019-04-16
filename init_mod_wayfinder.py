# pylint: disable=invalid-name
"""Digests mod-wayfinder json data."""
import csv, json, os, uuid, dataset


# directory
DATA_DIR = 'data'

# postgres
PG_USER = 'folio_admin'
PG_PASSWORD = 'folio_admin'
PG_NETLOC = '10.0.2.15'
PG_PORT = '5432'
PG_DBNAME = 'okapi_modules'
PG_URL = ("postgresql://" + PG_USER + ":" + PG_PASSWORD +
          '@' + PG_NETLOC + ':' + PG_PORT + '/' + PG_DBNAME)

#  wayfinder
WF_SCHEMA = 'diku_mod_wayfinder'
WF_SHELVES_TBL = 'shelves'
SHELVES_CSV_FILE = 'shelves.csv'
SHELVES_JSON_FILE = 'shelves.json'

# holdings record
HOLDINGS_SCHEMA = 'diku_mod_inventory_storage'
HOLDINGS_TBL = 'holdings_record'
MAIN_LOCATION_ID = 'fcd64ce1-6995-48f0-840e-89ffa2288371'
INSTANCE_IDS = [
    'b5b13415-145b-4e61-aaa8-aecf6a4a0571',
    'ef3641e5-ead0-4409-a485-4ab0059646c5',
    '0e3f5a3d-79c5-4252-96ea-6c0c0dbe4e7e',
    'de1c4934-f4dc-4ab1-8548-16915e682dd2',
    'b21d2059-dc52-4afa-b12c-6870f0680389'
]
CALL_NOS = [
    'CT502 .E542 1998',
    'F215 .W85 1951',
    'E302.6.F8 V362 1945',
    'UA23.15 .F45 2002',
    'D102 .M38 2002'
]


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
        json_file.write(json.dumps(shelves_csv, indent=4))


def populate_table(rows, tbl_name, schema_name, clear=True):
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
    new_obj['permanentLocationId'] = MAIN_LOCATION_ID
    new_obj['label'] = data['label']
    new_obj['lowerBound'] = data['lowerBound']
    new_obj['upperBound'] = data['upperBound']
    new_obj['mapTitle'] = data['mapTitle']
    new_obj['mapUri'] = data['mapUri']
    new_obj['x'] = data['x']
    new_obj['y'] = data['y']
    return dict(jsonb=new_obj)


def create_shelf_rows(shelves):
    """Creates shelf rows from json array."""
    print('Transforming data to database rows...')
    rows = []
    for shelf in shelves:
        row = create_shelf_row(shelf)
        rows.append(row)
    return rows


def update_holdings_record(row_id, call_no, tbl_name, schema_name):
    """Updates a holding record call number by instance id."""
    print("Updating call no. to {0} for row {1} in {2}.{3}...".format(
        call_no, row_id, schema_name, tbl_name))
    with dataset.Database(url=PG_URL, schema=schema_name) as db:
        tbl = db[tbl_name]
        row = tbl.find_one(instanceid=row_id)
        if row is not None:
            row['jsonb']['callNumber'] = call_no
            tbl.upsert(row, ['_id'])
    db.executable.close()
    db = None


def update_holdings_records(instance_ids, call_nos, tbl_name, schema_name):
    """Updates a batch of holding record call numbers by instance ids."""
    for instance_id, call_no in zip(instance_ids, call_nos):
        update_holdings_record(instance_id, call_no, tbl_name, schema_name)


if __name__ == '__main__':

    # transformed csv to json
    # csv_path = os.path.join(DATA_DIR, SHELVES_CSV_FILE)
    # json_path = os.path.join(DATA_DIR, SHELVES_JSON_FILE)
    # csv_to_json(csv_path, json_path)

    # update existing FOLIO holding records with call numbers
    update_holdings_records(INSTANCE_IDS, CALL_NOS,
                            HOLDINGS_TBL, HOLDINGS_SCHEMA)

    # load sample shelves into diku_mod_wayfinder.shelves
    shelves_path = os.path.join(DATA_DIR, SHELVES_JSON_FILE)
    shelves_json = load_json(shelves_path)
    shelf_rows = create_shelf_rows(shelves_json)
    populate_table(shelf_rows, WF_SHELVES_TBL, WF_SCHEMA)

    print('Complete...')
