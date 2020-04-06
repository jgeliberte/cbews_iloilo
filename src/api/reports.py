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
    data = request.form
    status = None
    message = ""

    try:
        report = data["html"]
        date = data["date"]

        email = "jason.ortega033@gmail.com"
        password = "ortegaJASON033"
        # send_to_email = data["email"]
        subject = "Field Survey : " + str(date)

        msg = MIMEMultipart()
        msg['From'] = email
        # msg['To'] = send_to_email
        msg['To'] = "jlouienepomuceno@gmail.com"
        msg['Subject'] = subject
        header = "<p>TEST</p>"
        footer = "<p>TEST</p>"
        # header = "<img src='http://cbewsl.com/assets/images/letter_header1.png' style='width: 100%'/><img src='http://cbewsl.com/assets/images/banner_new.png' style='width: 100%'/>"
        # footer = "<img src='http://cbewsl.com/assets/images/letter_footer1.png' style='width: 100%;  position: fixed; bottom: 0;'/>"
        paddingTop = "<div style='padding-top: 100px;'></div>"
        paddingBottom = "<div style='padding-top: 700px;'></div>"

        render_pdf = header+paddingTop+report+paddingBottom+footer

        pdfkit.from_string(render_pdf,'report.pdf')
        
        with open('report.pdf', 'rb') as f:
            mime = MIMEBase('image', 'png', filename='report.pdf')
            mime.add_header('Content-Disposition', 'attachment', filename='report.pdf')
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
        print(err)
        DB.session.rollback()
        status = False
        message = "No internet connection."

    feedback = {
        "status": status,
        "message": message
    }
    return jsonify(feedback)

