import argparse
import itertools
import queue
import re
import threading
import time
from typing import Iterable

class ConcurrentWebServerSimulation:
    def __init__(self, max_threads: int) -> None:
        self.max_threads = max_threads
        self._user_request_re = re.compile(
            r"^(create user|revoke user)\b|access rights|invalidate token",
            re.IGNORECASE,
        )
        self._download_re = re.compile(r"\bdownload\b", re.IGNORECASE)
        self._convert_re = re.compile(r"\bconvert\b", re.IGNORECASE)
        self._upload_re = re.compile(r"\bupload\b", re.IGNORECASE)

    def request_priority(self, request: str) -> int:
        # User management and access rights are highest priority.
        if self._user_request_re.search(request):
            return 0
        if self._download_re.search(request):
            return 2
        return 1

    def processing_time_seconds(self, request: str) -> float:
        # Simulate different processing durations per request type.
        if self._download_re.search(request):
            return 1.5
        if self._user_request_re.search(request):
            return 0.4
        if self._convert_re.search(request):
            return 1.1
        if self._upload_re.search(request):
            return 0.9
        return 0.7

    def enqueue_requests(self, requests: Iterable[str]) -> queue.PriorityQueue:
        # Stable ordering within the same priority using a monotonic counter.
        prioritized: queue.PriorityQueue[tuple[int, int, str]] = queue.PriorityQueue()
        counter = itertools.count()
        for request in requests:
            priority = self.request_priority(request)
            prioritized.put((priority, next(counter), request))
        return prioritized

    def process_requests(self, worker_name: str, requests: queue.PriorityQueue) -> None:
        while True:
            try:
                _, _, request = requests.get_nowait()
            except queue.Empty:
                # No more requests; worker can exit.
                return
            duration = self.processing_time_seconds(request)
            print(f"[{worker_name}] Processing: {request} ({duration:.1f}s)")
            time.sleep(duration)
            print(f"[{worker_name}] Done: {request}")
            requests.task_done()

    def run(self, requests: Iterable[str]) -> None:
        # Build the shared work queue so workers can pull tasks concurrently.
        requests_queue = self.enqueue_requests(requests)
        # Cap workers to avoid creating more threads than requests or the -n limit.
        max_threads = min(self.max_threads, requests_queue.qsize())
        threads = []
        for index in range(max_threads):
            worker_name = f"worker-{index + 1}"
            # Each worker runs process_requests and shares the same queue.
            thread = threading.Thread(
                target=self.process_requests,
                args=(worker_name, requests_queue),
                # Daemon threads exit automatically when the main thread exits.
                daemon=True,
            )
            # Start the thread immediately so it can begin fetching requests.
            thread.start()
            threads.append(thread)

        # Wait for all workers to finish before returning from run().
        for thread in threads:
            thread.join()


def main() -> None:
    parser = argparse.ArgumentParser(description="Concurrent Web Server Simulation")
    parser.add_argument(
        "-i",
        "--input",
        dest="input_file",
        required=True,
        help="Input file path",
    )
    parser.add_argument(
        "-n",
        type=int,
        required=True,
        help="Maximum number of threads that can be active at the same time",
    )
    args = parser.parse_args()

    if args.n < 1:
        parser.error("The number of threads must be at least 1.")

    with open(args.input_file, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle if line.strip()]

    if not lines:
        print("No requests to process.")
        return

    simulation = ConcurrentWebServerSimulation(args.n)
    simulation.run(lines)
    print("All requests processed.")
