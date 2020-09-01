import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate


def send_mail(subject, message):
    recipients = ["melvin@yellowstacks.nl"]

    for recipient in recipients:

        send_from = "melvin@yellowstacks.nl"
        send_to = recipient

        server = "smtp.office365.com"
        port = 587
        username = "melvin@yellowstacks.nl"
        password = "@bq4BE2kNqbl"
        use_tls = True

        msg = MIMEMultipart()
        msg["From"] = send_from
        msg["To"] = send_to
        msg["Date"] = formatdate(localtime=True)
        msg["Subject"] = subject

        msg.attach(MIMEText(message))

        smtp = smtplib.SMTP(server, port)
        if use_tls:
            smtp.starttls()
        smtp.login(username, password)
        smtp.sendmail(send_from, send_to, msg.as_string())
        smtp.quit()

    return "mail verzonden!"
