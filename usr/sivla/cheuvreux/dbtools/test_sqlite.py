import sqlite3
conn = sqlite3.connect('C:/simep/databases/se')
conn.row_factory = sqlite3.Row
c = conn.cursor()

# création de les tables: s'il vous plait TOUT EN MINUSCULE!!!
if 0:
    # table des estimateurs
    c.execute('create table estimator (estimator_id int, estimator_name varchar)') 
    c.execute('insert into estimator (estimator_id, estimator_name) values (1, "volume curve")')
    c.execute('insert into estimator (estimator_id, estimator_name) values (2, "market impact")')
    # table des contextes
    c.execute('create table context (estimator_id int, context_id int, context_name varchar)')
    c.execute('insert into context (estimator_id, context_id, context_name) values (1, 1, "usual day")')
    # table des valeurs
    c.execute('create table params (estimator_id int, context_id int, security_id int, trading_destination_id int, x_value int, value real)') 
    conn.commit()

#c.execute('delete from estimator')

c.execute('select * from estimator')
for row in c:
    print row
# print c.fetchall()

c.execute('select * from estimator where estimator_name = "volume curve"')
print c.fetchall()

# pour le remplissage à partir de MATLAB, cf se2sqlite
c.execute('select * from params where estimator_id = 1 and context_id = 1 and trading_destination_id = -1 and security_id = 110')
for row in c:
    print row

vals = {};
c.execute('select x_value, value from params where estimator_id = 1 and context_id = 1 and trading_destination_id = -1 and security_id = 110')
for row in c:
    print row['x_value'], '-->', row['value']
    vals[row['x_value']] = row['value']

