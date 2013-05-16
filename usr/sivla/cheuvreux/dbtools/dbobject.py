'''
Created on Oct 7, 2010

@author: syarc
'''


def save_object_to_db(db, object):
    '''
        Save an object into a database

        @param db: Database object
        @param object: Python object to save
    '''

    db.createTable(object.db_name(), object.db_fields())

    stmt = db.prepareInsertStmt(object.db_name(), object.header())
    db.execManyPreparedInsertStmt(stmt, object.get_fields())


def save_objects_to_db(db, objects):
    '''
        Save an object into a database

        @param db: Database object
        @param object: Python object to save
    '''
    if len(objects) == 0:
        return

    first = objects[0]
    if not db.isTableExist(first.db_name()):
        db.createTable(first.db_name(), ','.join(first.db_fields()))
        if hasattr(first, 'db_index'):
            index = first.db_index()
            if index:
                db.createIndex(index[0], first.db_name(), index[1], index[2])

    stmt = db.prepareInsertStmt(first.db_name(), first.header())

    def generator():
        for ob in objects:
            yield ob.get_fields()

    db.execManyPreparedInsertStmt(stmt, generator())


