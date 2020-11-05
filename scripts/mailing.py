import smtplib
import os.path as op
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders
import os


def send_mail(subject, message, files=None):
    recipients = ["2c0db8f8.zypp.io@emea.teams.ms"]

    for recipient in recipients:

        send_from = "melvin@yellowstacks.nl"
        send_to = recipient

        if not files:
            files = []
        else:
            None
        server = "smtp.office365.com"
        port = 587
        username = os.environ.get("EMAIL_USER")
        password = os.environ.get("EMAIL_PW")
        use_tls = True

        msg = MIMEMultipart()
        msg["From"] = send_from
        msg["To"] = send_to
        msg["Date"] = formatdate(localtime=True)
        msg["Subject"] = subject

        msg.attach(MIMEText(message))

        for path in files:
            part = MIMEBase("application", "octet-stream")
            with open(path, "rb") as file:
                part.set_payload(file.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition", 'attachment; filename="{}"'.format(op.basename(path))
            )
            msg.attach(part)

        smtp = smtplib.SMTP(server, port)
        if use_tls:
            smtp.starttls()
        smtp.login(username, password)
        smtp.sendmail(send_from, send_to, msg.as_string())
        smtp.quit()

        return "mail verzonden!"
