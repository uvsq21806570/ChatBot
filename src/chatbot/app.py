from flask import Flask, render_template, request

from response import bot_response

app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/get")
def bot_response():
    message = request.args.get("msgInput")
    if message:
        return bot_response(message)
    else:
        return "I did not understand what you were asking ..."


if __name__ == "__main__":
    app.run()
