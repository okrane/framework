'''
Created on Nov 2, 2010

@author: syarc
'''

def rational_approximation(x, maxden):
    ''' Find the rational approximation of the given
        real number.

        Based on C version:  http://www.ics.uci.edu/~eppstein/numth/frap.c

        @param x is the real number to approx
        @param maxdem is the maximum denomiator allowed
    '''

    startx = x
    # initialize matrix
    m = [[1,0],[0,1]]
    ai = int(x)
    while m[1][0] * ai + m[1][1] <= maxden:
        t = m[0][0] * ai + m[0][1]
        m[0][1] = m[0][0]
        m[0][0] = t
        t = m[1][0] * ai + m[1][1]
        m[1][1] = m[1][0]
        m[1][0] = t

        if x == ai:
            break

        x = 1.0 / (x - ai)



        if x > 0x7FFFFFFF:
            break

        ai = int(x)

    # now remaining x is between 0 and 1/ai
    # approx as either 0 or 1/m where m is max that will fit in maxden
    # first try zero
    sol1 = m[0][0], m[1][0], startx - float(m[0][0]) / float(m[1][0])

    # now try other possibility
    ai = (maxden - m[1][1]) / m[1][0];
    m[0][0] = m[0][0] * ai + m[0][1];
    m[1][0] = m[1][0] * ai + m[1][1];

    sol2 = m[0][0], m[1][0], startx - float(m[0][0]) / float(m[1][0])

    if sol1[0] + sol1[1] < sol2[0] + sol2[1] and sol1[0] > 0 and sol1[1] > 0:
        return sol1
    else:
        return sol2


if __name__ == '__main__':
    buy, sell = 8, 92
    round_buy = round(buy * 2, -1) / 2
    round_sell = round(sell * 2, -1) / 2
    print rational_approximation(round(buy / sell,2), 10)

