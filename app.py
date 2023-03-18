from flask import Flask, render_template
import os
import json
from flask_cors import CORS
import html

app = Flask(__name__)
CORS(app)

@app.route('/')
def home():
   return render_template('index.html')

@app.route('/loc/<name>')
def get_info(name):
    s1 = 'Hi there Mr ' + html.escape(name)
    return json.dumps(s1)


if __name__ == "__main__":
    app.run()
