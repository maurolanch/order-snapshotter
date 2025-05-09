import base64
import json
import os
import logging
from flask import Flask, request
from google.cloud import storage
from mercadolibre import get_order_snapshot
from datetime import datetime
import pytz

app = Flask(__name__)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

storage_client = storage.Client()
BUCKET_NAME = os.environ.get("SNAPSHOT_BUCKET", "ml-orders-snapshots")
TIMEZONE = pytz.timezone("America/Bogota")

@app.route("/", methods=["POST"])
def handle_pubsub():
    envelope = request.get_json()

    if not envelope or "message" not in envelope:
        logger.warning("No message in request")
        return "Bad Request", 400

    try:
        data = base64.b64decode(envelope["message"]["data"]).decode("utf-8")
        webhook = json.loads(data)
        order_id = webhook["resource"].split("/")[-1]
        logger.info(f"Procesando snapshot para order_id: {order_id}")
    except Exception as e:
        logger.error(f"Error parsing Pub/Sub message: {e}")
        return "Bad Request", 400

    try:
        snapshot = get_order_snapshot(order_id)
    except Exception as e:
        logger.error(f"Error al obtener snapshot: {e}")
        return "API Error", 500

    now = datetime.now(TIMEZONE)
    timestamp_str = now.strftime("%Y%m%dT%H%M%S%f")[:-3]
    path = (
        f"mercadolibre/order_snapshots/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"hour={now.hour:02d}/snapshot_{order_id}_{timestamp_str}.json"
    )

    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(path)
        blob.upload_from_string(json.dumps(snapshot), content_type="application/json")
        logger.info(f"Snapshot guardado en {path}")
    except Exception as e:
        logger.error(f"Error al subir snapshot: {e}")
        return "Storage Error", 500

    return "OK", 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
