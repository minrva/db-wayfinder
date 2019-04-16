# db-wayfinder

Copyright (C) 2016-2019 The Open Library Foundation

This software is distributed under the terms of the Apache License,
Version 2.0. See the file "[LICENSE](LICENSE)" for more information.

## Ingesting Mod-Wayfinder Sample Data

Ingesting mod-wayfinder sample data involves:

1. Running and enabling `mod-wayfinder` through Okapi, in order to trigger the creation of the `diku_mod_wayfinder` schema and `shelves` table.
1. Running `init_mod_wayfinder.py` which will automatically add uuidv4 ids to the json objects and then insert them into the `jsonb` column of their corresponding postgres datatable.

    ```bash
    source activate folio
    python ~/Desktop/folio/db/db-wayfinder/init_mod_wayfinder.py
    source deactivate folio
    ```
