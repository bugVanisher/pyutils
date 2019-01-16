#!/usr/bin/env python
# coding: utf-8
import email.utils
import logging
import os
import smtplib
import threading
from email.mime.text import MIMEText

from email.MIMEMultipart import MIMEMultipart

logger = logging.getLogger("maillog")


class MailBase(threading.Thread):
    mailServerPort = 25

    def __init__(self, subject, content, basic_info, attachment=""):
        """
                多线程邮件处理类
                @Params  target: file or string
                 basicInfo= {
                        "TOLIST": ["heyu@ucweb.com"],
                        "SERVER": "mail.ucweb.com",
                        "PORT": 25, #25 if missing
                        "USERNAME": "test@ucweb.com",
                        "PASSWORD": ""
                        }
                 (attachment)
        :param subject: 邮件标题
        :param content: 文件名或内容,文件名超过50字符
        :param basic_info: 邮件相关配置
        :param attachment: 附件
        """
        threading.Thread.__init__(self)
        self._set_basic_info(basic_info)
        self.subject = subject
        self.content = content
        self.attachment = attachment

    def _set_basic_info(self, basic_info):
        """

        :type basic_info: dict
        """
        self.BASICS = {}
        basic = ["TOLIST", "SERVER", "USERNAME", "PASSWORD", "PORT"]
        if isinstance(basic_info, dict):
            if "PORT" not in basic_info.keys():
                basic_info["PORT"] = self.mailServerPort
            if len(basic_info.keys()) != len(basic):
                logger.error("params nums not correct~")
                raise BadEmailSettings("basic_info param error")
            for basic in basic:
                if basic in basic_info.keys():
                    self.BASICS[basic] = basic_info[basic]
                else:
                    logger.error("mail settings has no %s", basic)
                    raise BadEmailSettings()
        else:
            logger.error("basic_info should be a dict")
            raise BadEmailSettings("basic_info not a dict")

    def _send_mail(self, subject, content, attachment):
        subject = subject.decode("utf-8")
        self._do_send_mail(self.BASICS["TOLIST"], subject, content, attachment)

    def run(self):
        if not self.subject or not self.content:
            return
        self._send_mail(self.subject, self.content, self.attachment)

    def _do_send_mail(self, to, subject, content, attachment):

        msg = MIMEMultipart('related')
        msg['To'] = ', '.join(to)
        msg['From'] = email.utils.formataddr((self.BASICS["USERNAME"], self.BASICS["USERNAME"]))
        msg['Subject'] = subject
        # msgText = MIMEText(content.encode("utf-8"), "html")
        msgtext = MIMEText(content, "html")
        msgtext.set_charset('utf-8')
        msg.attach(msgtext)
        if attachment:
            att = MIMEText(open(attachment, 'rb').read(), 'base64', 'utf-8')
            att["Content-Type"] = 'application/octet-stream'
            att["Content-Disposition"] = 'attachment;filename="%s"' % attachment
            msg.attach(att)
        server = smtplib.SMTP(self.BASICS["SERVER"], self.BASICS["PORT"])
        server.set_debuglevel(False)  # show communication with the server
        server.login(self.BASICS["USERNAME"], self.BASICS["PASSWORD"])
        try:
            server.sendmail(self.BASICS["USERNAME"], to, msg.as_string())
        finally:
            server.quit()


class FileMail(MailBase):
    """
        load文件发邮件
    """
    def __init__(self, subject, mail_file, basic_info, attachment=""):
        if len(mail_file) <= 50 and os.path.isfile(mail_file):
            fd = open(mail_file)
            content = fd.read()
            content = "<br/>".join(content.split("\n"))
            fd.close()
        else:
            content = ""
        super(FileMail, self).__init__(subject, content, basic_info, attachment)


class BadEmailSettings(Exception):
    pass
