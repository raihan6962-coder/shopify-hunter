import json
import csv
import io
import threading
import uuid
import time
from flask import Flask, render_template, request, Response, jsonify, stream_with_context
from hunter import run_hunt

app = Flask(__name__)

# Active jobs store (in-memory)
jobs = {}

class HuntJob:
    def __init__(self, job_id, keyword, location):
        self.job_id = job_id
        self.keyword = keyword
        self.location = location
        self.events = []
        self.done = False
        self.qualified = []
        self.all_results = []
        self.lock = threading.Lock()

    def push(self, data):
        with self.lock:
            self.events.append(data)

    def get_events_from(self, index):
        with self.lock:
            return self.events[index:]


def run_job(job: HuntJob):
    def cb(data):
        job.push(data)

    try:
        qualified, all_results = run_hunt(
            job.keyword, job.location,
            progress_callback=cb,
            max_stores=60
        )
        job.qualified = qualified
        job.all_results = all_results
    except Exception as e:
        job.push({"phase": "error", "msg": str(e), "pct": 100})
    finally:
        job.done = True
        job.push({"phase": "done", "pct": 100, "qualified_count": len(job.qualified)})


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/start", methods=["POST"])
def start():
    data = request.json
    keyword = data.get("keyword", "").strip()
    location = data.get("location", "").strip()
    if not keyword or not location:
        return jsonify({"error": "Keyword and location required"}), 400

    job_id = str(uuid.uuid4())
    job = HuntJob(job_id, keyword, location)
    jobs[job_id] = job

    thread = threading.Thread(target=run_job, args=(job,), daemon=True)
    thread.start()

    return jsonify({"job_id": job_id})


@app.route("/stream/<job_id>")
def stream(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404

    def generate():
        index = 0
        while True:
            events = job.get_events_from(index)
            for ev in events:
                yield f"data: {json.dumps(ev)}\n\n"
                index += 1
            if job.done and index >= len(job.events):
                break
            time.sleep(0.5)

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/results/<job_id>")
def results(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Not found"}), 404
    return jsonify({
        "qualified": job.qualified,
        "total_scanned": len(job.all_results),
        "done": job.done,
    })


@app.route("/export/<job_id>")
def export_csv(job_id):
    job = jobs.get(job_id)
    if not job or not job.qualified:
        return "No data", 404

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        "store_name", "url", "final_url", "emails", "phones",
        "has_payment", "payment_detected", "status"
    ])
    writer.writeheader()
    for r in job.qualified:
        writer.writerow({
            "store_name": r.get("store_name", ""),
            "url": r.get("url", ""),
            "final_url": r.get("final_url", ""),
            "emails": ", ".join(r.get("emails", [])),
            "phones": ", ".join(r.get("phones", [])),
            "has_payment": r.get("has_payment", False),
            "payment_detected": ", ".join(r.get("payment_detected", [])),
            "status": r.get("status", ""),
        })

    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=shopify_leads_{job_id[:8]}.csv"}
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000)
