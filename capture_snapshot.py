
import json
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

label = input("Enter snapshot label (normal/flood): ").strip().lower()

if label not in ("normal", "flood"):
    print("Enter only normal or flood")
else:
    payload = json.dumps({"label": label}).encode("utf-8")

    req = Request(
        "http://127.0.0.1:5000/api/comparison/capture",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with urlopen(req) as response:
            print(response.read().decode("utf-8"))
    except HTTPError as e:
        print("HTTP Error:", e.code)
        print(e.read().decode("utf-8"))
    except URLError as e:
        print("Connection error:", e.reason)
