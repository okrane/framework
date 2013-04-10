from simep.funcs.data.pyData import pyData



def parse_csv(filename, delimiter):
    return pyData('csv', filename = filename, delimiter = delimiter)


if __name__ == '__main__':
    print 'parsing'
    x = parse_csv('C:/st_repository/csv/FTE.PA/20111101.csv', delimiter = ';')
    print x['price'][1:10]
    print x['volume'][1:10]