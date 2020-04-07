"""
Inbox Functions Controller File
"""
import time
import pdfkit
import smtplib
import os
from flask import Blueprint, jsonify, request
from werkzeug import secure_filename
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os.path


REPORTS_BLUEPRINT = Blueprint("reports_blueprint", __name__)

@REPORTS_BLUEPRINT.route("/reports/send_email", methods=["POST"])
def field_survey_data_via_email():
    data = request.get_json()
    status = None
    message = ""

    try:
        report = data["html"]
        date = data["date"]
        subject = data["subject"]
        filename = data["filename"]
        send_to_email = data["email"]

        email = "dynaslopeswat@gmail.com"
        password = "dynaslopeswat"
        subject = "[TEST] : " + str(date)

        msg = MIMEMultipart()
        msg['From'] = email
        msg['To'] = send_to_email
        msg['Subject'] = subject
        header = "<img src='http://localhost/CBEWSL/MARIRONG/ASSETS/letter_header.png' style='width: 100%'/>"
        # header += "<img src='http://localhost/CBEWSL/MARIRONG/ASSETS/banner_new.png' style='width: 100%'/>"
        footer = "<img src='http://localhost/CBEWSL/MARIRONG/ASSETS/letter_footer.png' style='width: 100%;  position: fixed; bottom: 0;'/>"
        paddingTop = "<div style='padding-top: 100px;'></div>"
        paddingBottom = "<div style='padding-top: 700px;'></div>"

        render_pdf = header+paddingTop+report+paddingBottom+footer
        pdfkit.from_string(render_pdf,f'{filename}.pdf')
        with open(f'{filename}.pdf', 'rb') as f:
            mime = MIMEBase('image', 'png', filename=f'{filename}.pdf')
            mime.add_header('Content-Disposition', 'attachment', filename=f'{filename}.pdf')
            mime.add_header('X-Attachment-Id', '0')
            mime.add_header('Content-ID', '<0>')
            mime.set_payload(f.read())
            encoders.encode_base64(mime)
            msg.attach(mime)

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(email, password)
        text = msg.as_string()
        server.sendmail(email, send_to_email, text)
        server.quit()

        status = True
        message = "Email sent successfully!"
    except Exception as err:
        raise(err)
        status = False
        message = "No internet connection."

    feedback = {
        "status": status,
        "message": message
    }
    return jsonify(feedback)

