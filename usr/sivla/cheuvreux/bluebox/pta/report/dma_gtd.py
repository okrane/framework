'''
Created on Aug 17, 2012

@author: syarc
'''
from cheuvreux.bluebox.pta.report import DB
from cheuvreux.utils.dataset import Dataset, to_html, Quantity, Float
import sys
from cheuvreux.stdio.mail import HtmlEmail

def report(startdate, enddate, output):
    if not enddate:
        enddate = startdate
    
    query = ''' select order_id, expiry_datetime, entered_datetime, instrument_code, counterparty_code, 
                       volume_done, limit_price, trading_quantity
            from fidessa..[High Touch Orders Cumulative]
                where entered_by = 'DMA@CRAG.US' and tradedate >= '%s' and tradedate <= '%s' 
                  and substring(entered_datetime,1,8) < substring(expiry_datetime, 1,8)
                  and volume_done < trading_quantity 
                  and version = 1
            ''' % (startdate, enddate)
    rows = DB.select(query)
    
    
    ds = Dataset(['Order ID', 'Counterparty', 'Ticker', 'Limit Price', 'Volume Done', 'Quantity','Entered Datetime', 'Expiry Datetime'])
    ds.set_extra_style(['', "style='text-align:right;'"])
    for row in rows:
        ds.append([row[0], row[4], row[3], Float(row[6]), Quantity(row[5]), 
                   Quantity(row[7]), row[2][0:17], row[1][0:17]])
           
    print >> output, to_html(ds)

if __name__ == '__main__':
    
    email = HtmlEmail('smtpnotes')
    email.set_subject('DMA GTD orders')
    email.set_dest('sarchenault@cheuvreux.com')


    report('20120816','20120816', email)
    
    email.flush()