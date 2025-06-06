import os
from flask import Flask, render_template
import psycopg2
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

def get_connection():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PW"),
        dbname=os.environ.get("DB_NAME"),
        port=os.environ.get("DB_PORT", 5432),
    )

@app.route("/")
def options_feed():
    query = (
        "SELECT option_symbol, underlying_symbol, strike_price, call_put, expire_date "
        "FROM wb_opts LIMIT 25;"
    )
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return render_template("options.html", rows=rows)

if __name__ == "__main__":
    app.run(debug=True)
