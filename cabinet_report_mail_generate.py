"""
mail_generate.py

Sends the B2C report file via email to specified recipients using Yagmail.

Requirements:
- yagmail
"""

import yagmail
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Email Config
EMAIL_CONFIG = {
    'sender_email': 'your_email@gmail.com',
    'password': 'your_app_password',
    'receiver_emails': ['team@example.com', 'manager@example.com']
}

# Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MailSender")


def get_date_range():
    today = datetime.now()
    end_date = today - timedelta(days=1)
    start_date = end_date - relativedelta(months=1) + timedelta(days=1)
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')


def send_email_with_attachment(attachment_path, start_date, end_date):
    yag = yagmail.SMTP(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['password'])
    subject = "B2C Bikes Monthly Report"
    content = f"""
    [Automated Report]

    Dear Team,

    Please find the attached mileage and swap count report for B2C bikes from {start_date} to {end_date}.

    Regards,
    Oyika Data Team
    """
    try:
        yag.send(to=EMAIL_CONFIG['receiver_emails'], subject=subject, contents=content, attachments=attachment_path)
        logger.info("📧 Email sent successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to send email: {e}")


if __name__ == "__main__":
    start_date, end_date = get_date_range()
    report_path = f"{os.path.expanduser('~/shared_reports')}/B2C_3_Bikes_Report_{start_date}_to_{end_date}.csv"
    send_email_with_attachment(report_path, start_date, end_date)
