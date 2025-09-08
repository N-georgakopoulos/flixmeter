from flask import Flask, request, jsonify
import script  # assumes you have script.py in same folder

app = Flask(__name__)

@app.route("/")
def home():
    return "API is running!"

@app.route("/run", methods=["POST"])
def run():
    data = request.json or {}
    result = script.main(data)  # make sure script.py has a main() function
    return jsonify({"result": result})
