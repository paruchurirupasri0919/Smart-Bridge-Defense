from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from flask import send_file
from flask import Flask, render_template, jsonify, request
from collections import deque
from threading import Lock, Thread, Event
from time import time
import random

app = Flask(__name__)

lock = Lock()
stop_event = Event()

RATE_LIMIT = 30
QUEUE_CAPACITY = 50
WINDOW_SECONDS = 10

events = deque(maxlen=100)
history = deque(maxlen=60)

comparison_snapshots = {
    "normal": None,
    "flood": None
}


receivers = {
    "RX001": {
        "id": "RX001", "name": "North Demo Receiver",
        "location": "Delhi", "lat": 28.6139, "lon": 77.2090
    },
    "RX002": {
        "id": "RX002", "name": "West Demo Receiver",
        "location": "Mumbai", "lat": 19.0760, "lon": 72.8777
    },
    "RX003": {
        "id": "RX003", "name": "South Demo Receiver",
        "location": "Bengaluru", "lat": 12.9716, "lon": 77.5946
    },
    "RX004": {
        "id": "RX004", "name": "East Demo Receiver",
        "location": "Kolkata", "lat": 22.5726, "lon": 88.3639
    },
    "RX005": {
        "id": "RX005", "name": "AP Demo Receiver",
        "location": "Vijayawada", "lat": 16.5062, "lon": 80.6480
    }
}


def reset_receiver(r):
    r.update({
        "mode": "normal",
        "status": "NORMAL",
        "attack_detected": False,
        "previous_attack_detected": False,
        "packets_received": 0,
        "accepted_packets": 0,
        "dropped_packets": 0,
        "invalid_packets": 0,
        "crc_errors": 0,
        "auth_failures": 0,
        "pps": 0,
        "window_pps": 0,
        "window_crc_errors": 0,
        "window_auth_failures": 0,
        "crc_error_rate": 0,
        "auth_failure_rate": 0,
        "queue_size": 0,
        "message": "Normal simulated traffic",
        "queue": deque(),
        "packet_times": deque(),
        "observations": deque()
    })


for receiver in receivers.values():
    reset_receiver(receiver)



def simulate_receiver(r, now):
    # 1. Generate simulated traffic
    flood = r["mode"] == "flood"
    rate = random.randint(100, 160) if flood else random.randint(5, 12)

    invalid = (
        random.randint(int(rate * 0.75), int(rate * 0.95))
        if flood else 0
    )
    valid = rate - invalid

    crc_errors = random.randint(1, max(1, invalid // 4)) if flood else 0
    auth_failures = random.randint(1, max(1, invalid // 4)) if flood else 0

    r["pps"] = rate
    r["packets_received"] += rate
    r["invalid_packets"] += invalid
    r["crc_errors"] += crc_errors
    r["auth_failures"] += auth_failures

    # 2. Maintain sliding window
    observations = r["observations"]
    observations.append({
        "time": now,
        "packets": rate,
        "crc": crc_errors,
        "auth": auth_failures
    })

    while observations and now - observations[0]["time"] > WINDOW_SECONDS:
        observations.popleft()

    window_packets = sum(x["packets"] for x in observations)
    window_crc = sum(x["crc"] for x in observations)
    window_auth = sum(x["auth"] for x in observations)

    duration = max(
        1,
        min(WINDOW_SECONDS, now - observations[0]["time"])
    )

    r["window_pps"] = round(window_packets / duration, 2)
    r["window_crc_errors"] = window_crc
    r["window_auth_failures"] = window_auth

    r["crc_error_rate"] = round(
        100 * window_crc / max(1, window_packets), 2
    )
    r["auth_failure_rate"] = round(
        100 * window_auth / max(1, window_packets), 2
    )

    # 3. Detect anomalies from measured simulation metrics
    attack = (
        r["window_pps"] > RATE_LIMIT
        or r["crc_error_rate"] > 10
        or r["auth_failure_rate"] > 10
    )

    r["attack_detected"] = attack
    r["status"] = "UNDER ATTACK" if attack else "NORMAL"

    if attack:
        r["message"] = "Anomaly detected; rate limiting active."
    else:
        r["message"] = "Traffic within configured thresholds."

    # 4. Process queued packets
    processing_capacity = 8 if attack else 12

    for _ in range(min(len(r["queue"]), processing_capacity)):
        r["queue"].popleft()

    # 5. Rate-limit traffic and enforce queue capacity
    allowed = min(valid, RATE_LIMIT) if attack else valid
    available = max(0, QUEUE_CAPACITY - len(r["queue"]))
    accepted = min(allowed, available)

    for _ in range(accepted):
        r["queue"].append(now)

    dropped = invalid + (valid - accepted)

    r["accepted_packets"] += accepted
    r["dropped_packets"] += dropped
    r["queue_size"] = len(r["queue"])

    # 6. Record attack event
        # 6. Record attack and recovery transitions
    previous_attack = r["previous_attack_detected"]

    if attack and not previous_attack:
        events.appendleft({
            "time": round(now, 2),
            "receiver_id": r["id"],
            "event": "ANOMALY_DETECTED",
            "details": (
                f"Window PPS: {r['window_pps']}; "
                f"CRC error rate: {r['crc_error_rate']}%; "
                f"Auth failure rate: {r['auth_failure_rate']}%"
            )
        })

    elif not attack and previous_attack:
        events.appendleft({
            "time": round(now, 2),
            "receiver_id": r["id"],
            "event": "RECOVERY",
            "details": "Traffic returned within configured thresholds."
        })

    r["previous_attack_detected"] = attack

    # 7. Update live chart history
    history.append({
        "time": round(now, 2),
        "receiver_id": r["id"],
        "pps": rate,
        "dropped": r["dropped_packets"]
    })

  



def simulation_worker():
    while not stop_event.is_set():
        now = time()
        try:
            with lock:
                for r in receivers.values():
                    simulate_receiver(r, now)
        except Exception as exc:
            print("SIMULATION ERROR:", repr(exc))

        stop_event.wait(1)


def public_receiver(r):
    # Exclude every deque; none can reach jsonify().
    return {
        key: value
        for key, value in r.items()
        if not isinstance(value, deque)
    }


@app.route("/")
def home():
    return render_template("index.html")



    
@app.route("/api/status")
def api_status():
    with lock:
        safe_receivers = {
            rid: public_receiver(r)
            for rid, r in receivers.items()
        }

        receiver_list = list(receivers.values())

        totals = {
            "receiver_count": len(receiver_list),
            "packets_received": sum(
                r["packets_received"] for r in receiver_list
            ),
            "accepted_packets": sum(
                r["accepted_packets"] for r in receiver_list
            ),
            "dropped_packets": sum(
                r["dropped_packets"] for r in receiver_list
            ),
            "invalid_packets": sum(
                r["invalid_packets"] for r in receiver_list
            ),
            "crc_errors": sum(
                r["crc_errors"] for r in receiver_list
            ),
            "auth_failures": sum(
                r["auth_failures"] for r in receiver_list
            ),
            "packets_per_second": sum(
                r["pps"] for r in receiver_list
            ),
            "queue_size": sum(
                r["queue_size"] for r in receiver_list
            ),
            "queue_capacity": QUEUE_CAPACITY * len(receiver_list),
            "under_attack_receivers": sum(
                1 for r in receiver_list
                if r["attack_detected"]
            )
        }

        totals["queue_utilization"] = round(
            100 * totals["queue_size"]
            / max(1, totals["queue_capacity"]),
            2
        )

        totals["drop_rate"] = round(
            100 * totals["dropped_packets"]
            / max(1, totals["packets_received"]),
            2
        )

        return jsonify({
            "data_source": "SIMULATION",
            "receivers": safe_receivers,
            "events": list(events),
            "history": list(history),
            "totals": totals,
            "active_attacks": totals["under_attack_receivers"]
        })


@app.route("/api/comparison", methods=["GET"])
def api_comparison():
    with lock:
        result = []

        for r in receivers.values():
            total = r["packets_received"]
            accepted = r["accepted_packets"]
            dropped = r["dropped_packets"]

            result.append({
                "receiver_id": r["id"],
                "location": r["location"],
                "mode": r["mode"],
                "status": r["status"],
                "packets_received": total,
                "accepted_packets": accepted,
                "dropped_packets": dropped,
                "drop_rate": round(
                    100 * dropped / max(1, total), 2
                ),
                "queue_size": r["queue_size"],
                "queue_capacity": QUEUE_CAPACITY,
                "queue_utilization": round(
                    100 * r["queue_size"] / QUEUE_CAPACITY, 2
                ),
                "attack_detected": r["attack_detected"]
            })

        return jsonify({
            "data_source": "SIMULATION",
            "comparison": result
        })

@app.route("/api/comparison/capture", methods=["POST"])
def capture_comparison():
    data = request.get_json(silent=True) or {}
    label = data.get("label")

    if label not in ("normal", "flood"):
        return jsonify({
            "error": "Label must be normal or flood"
        }), 400

    with lock:
        snapshot = []

        for r in receivers.values():
            snapshot.append({
                "receiver_id": r["id"],
                "location": r["location"],
                "mode": r["mode"],
                "status": r["status"],
                "packets_received": r["packets_received"],
                "accepted_packets": r["accepted_packets"],
                "dropped_packets": r["dropped_packets"],
                "drop_rate": round(
                    100 * r["dropped_packets"]
                    / max(1, r["packets_received"]), 2
                ),
                "queue_size": r["queue_size"],
                "queue_capacity": QUEUE_CAPACITY,
                "queue_utilization": round(
                    100 * r["queue_size"] / QUEUE_CAPACITY, 2
                ),
                "attack_detected": r["attack_detected"]
            })

        comparison_snapshots[label] = snapshot

    return jsonify({
        "message": f"{label.title()} snapshot captured",
        "label": label,
        "count": len(snapshot),
        "data_source": "SIMULATION"
    })


@app.route("/api/comparison/results", methods=["GET"])
def comparison_results():
    with lock:
        normal = comparison_snapshots.get("normal")
        flood = comparison_snapshots.get("flood")

        if normal is None or flood is None:
            return jsonify({
                "error": "Capture both normal and flood snapshots first",
                "normal_captured": normal is not None,
                "flood_captured": flood is not None
            }), 400

        normal_by_id = {
            item["receiver_id"]: item for item in normal
        }
        flood_by_id = {
            item["receiver_id"]: item for item in flood
        }

        results = []

        for receiver_id, n in normal_by_id.items():
            f = flood_by_id.get(receiver_id)

            if f is None:
                continue

            results.append({
                "receiver_id": receiver_id,
                "location": n["location"],
                "normal_drop_rate": n["drop_rate"],
                "flood_drop_rate": f["drop_rate"],
                "normal_packets": n["packets_received"],
                "flood_packets": f["packets_received"],
                "normal_dropped": n["dropped_packets"],
                "flood_dropped": f["dropped_packets"],
                "flood_attack_detected": f["attack_detected"],
                "normal_queue_utilization": n["queue_utilization"],
                "flood_queue_utilization": f["queue_utilization"]
            })

        return jsonify({
            "data_source": "SIMULATION",
            "count": len(results),
            "comparison": results
        })

from io import StringIO
import csv
from flask import Response

@app.route("/api/comparison/export", methods=["GET"])
def export_comparison():
    with lock:
        normal = comparison_snapshots.get("normal")
        flood = comparison_snapshots.get("flood")

        if normal is None or flood is None:
            return jsonify({
                "error": "Capture both snapshots first"
            }), 400

        normal_by_id = {x["receiver_id"]: x for x in normal}
        flood_by_id = {x["receiver_id"]: x for x in flood}

        output = StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "Receiver ID", "Location",
            "Normal Drop Rate", "Flood Drop Rate",
            "Normal Queue", "Flood Queue",
            "Attack Detected"
        ])

        for rid, n in normal_by_id.items():
            f = flood_by_id.get(rid)
            if f is not None:
                writer.writerow([
                    rid, n["location"],
                    n["drop_rate"], f["drop_rate"],
                    n["queue_utilization"],
                    f["queue_utilization"],
                    f["attack_detected"]
                ])

        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={
                "Content-Disposition":
                "attachment; filename=bridge_comparison_report.csv"
            }
        )

@app.route("/api/comparison/pdf", methods=["GET"])
def export_comparison_pdf():
    with lock:
        normal = comparison_snapshots.get("normal")
        flood = comparison_snapshots.get("flood")

        if normal is None or flood is None:
            return jsonify({
                "error": "Capture both snapshots first"
            }), 400

        normal_by_id = {
            x["receiver_id"]: x for x in normal
        }
        flood_by_id = {
            x["receiver_id"]: x for x in flood
        }

        buffer = BytesIO()
        pdf = canvas.Canvas(buffer, pagesize=A4)
        width, height = A4

        y = height - 50
        pdf.setFont("Helvetica-Bold", 16)
        pdf.drawString(40, y, "Smart Bridge Defense Report")

        y -= 25
        pdf.setFont("Helvetica", 10)
        pdf.drawString(40, y, "Data source: Software simulation")

        y -= 30

        for rid, n in normal_by_id.items():
            f = flood_by_id.get(rid)
            if f is None:
                continue

            if y < 100:
                pdf.showPage()
                y = height - 50

            pdf.setFont("Helvetica-Bold", 11)
            pdf.drawString(
                40, y, f"Receiver: {rid} - {n['location']}"
            )
            y -= 18

            pdf.setFont("Helvetica", 10)
            pdf.drawString(
                50, y,
                f"Normal drop: {n['drop_rate']}% | "
                f"Flood drop: {f['drop_rate']}%"
            )
            y -= 16

            pdf.drawString(
                50, y,
                f"Normal queue: {n['queue_utilization']}% | "
                f"Flood queue: {f['queue_utilization']}%"
            )
            y -= 16

            attack = "YES" if f["attack_detected"] else "NO"
            pdf.drawString(50, y, f"Attack detected: {attack}")
            y -= 25

        pdf.save()
        buffer.seek(0)

    return send_file(
        buffer,
        mimetype="application/pdf",
        as_attachment=True,
        download_name="smart_bridge_security_report.pdf"
    )

@app.route("/api/mode", methods=["POST"])
def set_mode():
    data = request.get_json(silent=True) or {}
    mode = data.get("mode")
    receiver_id = data.get("receiver_id", "ALL")

    if mode not in ("normal", "flood"):
        return jsonify({
            "error": "mode must be normal or flood"
        }), 400

    with lock:
        if receiver_id == "ALL":
            targets = list(receivers.values())
        elif receiver_id in receivers:
            targets = [receivers[receiver_id]]
        else:
            return jsonify({
                "error": "Unknown receiver"
            }), 404

        for r in targets:
            r["mode"] = mode

            if mode == "normal":
                r["attack_detected"] = False
                r["status"] = "NORMAL"

    return jsonify({
        "message": "Mode updated",
        "mode": mode,
        "receiver_id": receiver_id
    })



@app.route("/api/reset", methods=["POST"])
def reset():
    with lock:
        for r in receivers.values():
            reset_receiver(r)
            
        events.clear()
        history.clear()

    return jsonify({"message": "Simulation reset"})


if __name__ == "__main__":
    Thread(target=simulation_worker, daemon=True).start()
    app.run(host="127.0.0.1", port=5000, debug=False, use_reloader=False, threaded=True)
