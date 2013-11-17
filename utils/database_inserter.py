def insert_batch(batch, db_collection):
    db_collection.insert(batch, w=0,
                         j=False, fsync=False,
                         continue_on_error=True)