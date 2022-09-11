from flask import Flask
from flask_wtf.csrf import CSRFProtect

app = Flask(__name__)
csrf = CSRFProtect()
csrf.init_app(app)

@app.route('/')
def hello():
    html_page = '<html><title>Sample flask website</title><body><h2>This is a sample website created with Flask and Python</h2></body></html>';
    return html_page;

