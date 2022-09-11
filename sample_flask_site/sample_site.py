from flask import Flask

app = Flask(__name__)

@app.route('/')
def hello():
    html_page = '<html><title>Sample flask website</title><body><h2>This is a sample website created with Flask and Python</h2></body></html>';
    return html_page;

