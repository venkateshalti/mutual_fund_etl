# the following code simulates an online service without depending on 3rd party services
# host the service on another virtual environment/interpreter to avoid conflict
from flask import Flask  # install flask on the other virtual environment, version 2.1.2 works well
from flask import send_file
app = Flask(__name__)

@app.route('/download')  # path is http://127.0.0.1:5000/download
def downloadFile ():
    return send_file(path, as_attachment=True)

if __name__ == '__main__':
    app.run(port=5000,debug=True)