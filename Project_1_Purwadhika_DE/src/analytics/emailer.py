import smtplib
from email.message import EmailMessage
from .config import Settings
from .logger import Logger
import os

class Emailer:
    @staticmethod
    def send_email(subject, body, attachment_path):
        if not Settings.EMAIL_TO or not Settings.EMAIL_FROM or not Settings.EMAIL_PASSWORD:
            Logger.log("EMAIL_TO / EMAIL_FROM / EMAIL_PASSWORD belum di-set, skip sending email.", "WARNING")
            return
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = Settings.EMAIL_FROM
        msg["To"] = Settings.EMAIL_TO
        msg.set_content(body)

        with open(attachment_path, "rb") as f:
            file_data = f.read()
            file_name = os.path.basename(attachment_path)
            msg.add_attachment(file_data, maintype="application", subtype="zip", filename=file_name)

        try:
            with smtplib.SMTP(Settings.SMTP_SERVER, Settings.SMTP_PORT) as server:
                server.starttls()
                server.login(Settings.EMAIL_FROM, Settings.EMAIL_PASSWORD)
                server.send_message(msg)
                Logger.log(f"✓ Email sent successfully to {Settings.EMAIL_TO} with attachment {file_name}")
        except Exception as e:
            Logger.log(f"ERROR sending email: {e}", "ERROR")
