import smtplib
from email.message import EmailMessage
from .config import Settings
from .logger import Logger
import os

class Emailer:
    """
    Class untuk mengirim email dengan attachment menggunakan konfigurasi dari Settings.

    Attributes:
        None (semua konfigurasi diambil dari Settings)
    """

    @staticmethod
    def send_email(subject: str, body: str, attachment_path: str):
        """
        Mengirim email dengan subject, body, dan attachment.

        Parameters:
            subject (str): Judul email.
            body (str): Isi email.
            attachment_path (str): Path file yang akan dilampirkan.

        Behavior:
            - Mengecek apakah EMAIL_TO, EMAIL_FROM, dan EMAIL_PASSWORD sudah di-set.
              Jika belum, log warning dan skip.
            - Membuat email dengan attachment (ZIP atau file lain) menggunakan EmailMessage.
            - Mengirim email melalui SMTP server sesuai Settings.
            - Menangani error dan mencatatnya di log.

        Returns:
            None
        """
        if not Settings.EMAIL_TO or not Settings.EMAIL_FROM or not Settings.EMAIL_PASSWORD:
            Logger.log(
                "EMAIL_TO / EMAIL_FROM / EMAIL_PASSWORD belum di-set, skip sending email.",
                "WARNING"
            )
            return

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = Settings.EMAIL_FROM
        msg["To"] = Settings.EMAIL_TO
        msg.set_content(body)

        # Attach file
        try:
            with open(attachment_path, "rb") as f:
                file_data = f.read()
                file_name = os.path.basename(attachment_path)
                msg.add_attachment(
                    file_data, 
                    maintype="application", 
                    subtype="zip", 
                    filename=file_name
                )
        except Exception as e:
            Logger.log(f"ERROR reading attachment {attachment_path}: {e}", "ERROR")
            return

        # Kirim email
        try:
            with smtplib.SMTP(Settings.SMTP_SERVER, Settings.SMTP_PORT) as server:
                server.starttls()
                server.login(Settings.EMAIL_FROM, Settings.EMAIL_PASSWORD)
                server.send_message(msg)
                Logger.log(f"✓ Email sent successfully to {Settings.EMAIL_TO} with attachment {file_name}")
        except Exception as e:
            Logger.log(f"ERROR sending email: {e}", "ERROR")
