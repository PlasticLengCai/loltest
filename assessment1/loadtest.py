import os, sys, time, threading, requests

BASE = "http://ec2-16-176-128-122.ap-southeast-2.compute.amazonaws.com"
TOKEN = os.environ.get("TOKEN","")
IDS = [int(x) for x in sys.argv[1:]]
HEAD = {"Authorization":"Bearer "+TOKEN}

def worker(vid):
    while True:
        try:
            requests.post(f"{BASE}/transcode/{vid}", headers=HEAD, timeout=10)
        except Exception:
            pass

threads = []
for vid in IDS:
    for _ in range(3):
        t = threading.Thread(target=worker, args=(vid,), daemon=True)
        t.start()
        threads.append(t)

while True:
    time.sleep(1)

