# pylint: disable=no-member
# utf-8
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.encoders import encode_base64
from infraestructure.conf import getConf
from utils.read_params import ReadParams

class Email:
    """
    Class that allow send email
    """
    def __init__(self,
                 params: ReadParams,
                 conf: getConf,
                 subject='',
                 body='',
                 email_from=None,
                 email_to=None):
        self.email_from = params.email_from \
            if email_from is None else email_from
        self.email_to = params.email_to \
            if email_to is None else email_to
        self.subject = subject
        self.body = body
        self.smtp_server = conf.SMPTConf.host
        self.logger = logging.getLogger('email')

    def send_email(self, msg):
        """
        Method [ send_email ] create a SMTP instance with host ip in
        [ smpt_server ] instance variable and send email in msg argument
        Param [ msg ] is a MIME object ready to send
        """
        server = smtplib.SMTP(self.smtp_server)
        server.sendmail(self.email_from,
                        self.email_to,
                        msg.as_string())

    def send_email_with_csv(self, data_to_send):
        """
        Method [send_email_with_csv] send a email with one or multiples
        csv files attached to recipients.
        Param [ data_to send ] is a array of tuple composed by
                (name_file_send, dataframe_to_csv)
            Param [name_file_send] is the name of file,
                must contain the extension ".csv"
            Param [dataframe_to_csv] is a pandas dataframe with data that
                will be saved as a csv and sent
        """
        self.logger.info('Preparing email')
        msg = MIMEMultipart('mixed')
        msg['Subject'] = self.subject
        msg['From'] = self.email_from
        msg['To'] = ", ".join(self.email_to)
        msg.attach(MIMEText(self.body, 'plain'))
        for file in data_to_send:
            self.logger.info('Charging files')
            file[1].to_csv(file[0], sep=";", index=False)
            part = MIMEBase('application', "octet-stream")
            part.set_payload(open(file[0], "rb").read())
            encode_base64(part)
            part.add_header('Content-Disposition',
                            'attachment', filename=file[0])
            msg.attach(part)
            self.logger.info('File {} charged'.format(file[0]))
        self.send_email(msg)

    def send_email_with_excel(self, data_to_send):
        """
        Method [send_email_with_excel] send a email with one or multiples
        excel files attached to recipients.
        Param [ data_to send ] is a array of tuple composed by
                (name_file_send, dataframe_to_excel)
            Param [name_file_send] is the name of file,
                must contain the extension ".xls"
            Param [dataframe_to_excel] is a pandas dataframe with data that
                will be saved as a xls and sent
        """
        self.logger.info('Preparing email')
        msg = MIMEMultipart('mixed')
        msg['Subject'] = self.subject
        msg['From'] = self.email_from
        msg['To'] = ", ".join(self.email_to)
        msg.attach(MIMEText(self.body, 'plain'))
        for file in data_to_send:
            self.logger.info('Charging files')
            file[1].to_excel(file[0], index=False)
            part = MIMEBase('application', "octet-stream")
            part.set_payload(open(file[0], "rb").read())
            encode_base64(part)
            part.add_header('Content-Disposition',
                            'attachment', filename=file[0])
            msg.attach(part)
            self.logger.info('File {} charged'.format(file[0]))
        self.send_email(msg)
