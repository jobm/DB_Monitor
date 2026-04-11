import argparse
import concurrent.futures
import time
import requests
import json
import statistics

BASE_URL = "http://localhost:8000"

def fetch_events(admin_key):
    headers = {"X-API-Key": admin_key}
    start = time.perf_counter()
    try:
        resp = requests.get(f"{BASE_URL}/events?limit=50", headers=headers, timeout=5)
        resp.raise_for_status()
        success = True
    except Exception as e:
        success = False
    duration = time.perf_counter() - start
    return success, duration

def fetch_stats(admin_key):
    headers = {"X-API-Key": admin_key}
    start = time.perf_counter()
    try:
        resp = requests.get(f"{BASE_URL}/events/stats", headers=headers, timeout=5)
        resp.raise_for_status()
        success = True
    except Exception as e:
        success = False
    duration = time.perf_counter() - start
    return success, duration

def load_test(workers, requests_per_worker):
    print(f"Starting load test on DB Monitor API.")
    print(f"Workers: {workers}, Requests/Worker: {requests_per_worker}")

    admin_key = None
    # Assuming bootstrap was already run in previous tests
    # Best effort find API key or prompt
    print("Warning: Load test assumes system has an active Admin key already deployed unless bootstrap hasn't occurred.")
    resp = requests.post(f"{BASE_URL}/auth/bootstrap?owner_name=load_tester")
    if resp.status_code == 200:
        admin_key = resp.json().get("api_key")
        print("Bootstrapped load testing admin key successfully.")
    else:
        print("Note: If API rejects your calls, you must configure a viewer or admin key inside this script.")

    if not admin_key:
        print("Please input an admin or viewer API key to run load tests:")
        # We simulate passing the api key since interactive input won't work in automated environment
        admin_key = "PLEASE_SET_KEY_HERE"

    if admin_key == "PLEASE_SET_KEY_HERE":
        print("Ensure you modify load_test.py with a valid API key. Running unprotected endpoint healthcheck instead.")
        task = lambda: requests.get(f"{BASE_URL}/health", timeout=5).status_code == 200
    else:
        # We will randomly distribute load between the two heaviest endpoints
        import random
        task = lambda: random.choice([fetch_events, fetch_stats])(admin_key)

    success_count = 0
    failure_count = 0
    response_times = []

    def load_task(_):
        if admin_key == "PLEASE_SET_KEY_HERE":
            # the healthcheck mock
            start = time.perf_counter()
            s = task()
            dur = time.perf_counter() - start
            return s, dur
        else:
            return task()

    start_time = time.perf_counter()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(load_task, i) for i in range(workers * requests_per_worker)]
        for future in concurrent.futures.as_completed(futures):
            success, duration = future.result()
            if success:
                success_count += 1
                response_times.append(duration)
            else:
                failure_count += 1

    end_time = time.perf_counter()
    duration = end_time - start_time
    tps = (success_count + failure_count) / duration

    print("\n--- Load Test Results ---")
    print(f"Total Time:   {duration:.2f} s")
    print(f"Successful:   {success_count}")
    print(f"Failed:       {failure_count}")
    print(f"TPS (Req/s):  {tps:.2f}")

    if response_times:
        print(f"Avg Response: {statistics.mean(response_times) * 1000:.2f} ms")
        print(f"Max Response: {max(response_times) * 1000:.2f} ms")
        print(f"Min Response: {min(response_times) * 1000:.2f} ms")
        print(f"P95 Response: {statistics.quantiles(response_times, n=100)[94] * 1000:.2f} ms")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load test DB monitor endpoints.")
    parser.add_argument("--workers", type=int, default=10, help="Number of concurrent workers")
    parser.add_argument("--requests", type=int, default=50, help="Requests per worker")
    args = parser.parse_args()

    load_test(args.workers, args.requests)
