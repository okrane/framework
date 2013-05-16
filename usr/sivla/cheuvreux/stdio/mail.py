'''
Created on May 12, 2010

@author: syarc
'''

import os
import sys
from email import Encoders
from smtplib import SMTP
from cheuvreux.stdio.html import table_style

#Import differs from python 2.4,2.3 and 2.6
if sys.version_info[0] == 2 and sys.version_info[1] in (3,4):
    from email.MIMEText import MIMEText
    from email.MIMEMultipart import MIMEMultipart
    from email.MIMEImage import MIMEImage
    from email.MIMEBase import MIMEBase
else:
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.mime.image import MIMEImage
    from email.mime.base import MIMEBase




class Email(object):
    def __init__(self, smtp_server, username=None, password=None):
        self._smpt = smtp_server
        self._username = username
        self._password = password
        self._buffer = []
        self._sender = None
        self._type = 'plain'
        self._subject = None
        self._images = []
        self._attachments = []
        self._dest = None

    def subject(self):
        return self._subject

    def sender(self):
        return self._sender

    def type(self):
        return self._type

    def set_subject(self, subject):
        self._subject = subject

    def set_sender(self, sender):
        self._sender = sender

    def set_type(self, type):
        self._type = type

    def set_dest(self, dest):
        self._dest = dest

    def attachFile(self, file, extra_headers=None):
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(open(file, 'rb').read())
        Encoders.encode_base64(part)
        part.add_header("Content-Disposition", 'attachment', filename=os.path.basename(file));
        if extra_headers:
            for (key, value) in extra_headers.items():
                part.add_header(key, value, name=os.path.basename(file))

        self._attachments.append(part)

    def attachImage(self, img, content_id=None):
        fp = open(img, 'rb')
        img = MIMEImage(fp.read())
        fp.close()

        if content_id is not None:
            img.add_header('Content-ID', '<%s>' % content_id)

        self._images.append(img)

    def write(self, string):
        self._buffer.append(str(string))

    def flush(self):

        if self._dest:
            self.send(self._dest)

    def send(self, dest):
        msg = MIMEMultipart('related')
        txt = MIMEText(''.join(self._buffer), self._type)
        #txt.set_charset('utf8')
        if self._subject is not None:
            msg['Subject'] = self._subject
        if self._sender is not None:
            msg['From'] = self._sender

        msg["To"] = dest

        msg.attach(txt)

        for img in self._images:
            msg.attach(img)

        for file in self._attachments:
            msg.attach(file)

        conn = None
        try:
            conn = SMTP(self._smpt)
            if self._username is not None and self._password is not None:
                conn.login(self._username, self._password)

            conn.sendmail(self._sender, dest.split(','), msg.as_string())

        finally:
            if conn:
                conn.close()


class HtmlEmail(Email):
    def __init__(self, smtp_server, username=None, password=None, style=None, font_size='10pt'):
        Email.__init__(self,smtp_server, username, password)

        self._type = 'html'
        self.write('<html>\n')
        if style:
            self.write('<head>\n' + style(font_size) + '\n</head>\n')
        else:
            self.write('<head>\n' + table_style() + '\n</head>\n')
        self.write('<body>\n')

    def send(self, dest):
        self.write('</body>\n</html>\n')
        Email.send(self, dest)
