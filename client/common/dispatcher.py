import asyncio
import json
import time
from datetime import datetime


class Dispatcher:
    """
    Generic helper class for handling concurrency in ACKS.
    When publishing CAN data, there are 3 important settings:

    Semaphore window: amount of in-flight messages awaiting ACK. If all slots are full, new sends need to wait for slot to be freed.
        - Adding too big window overloads server, increasing tail latency

    Workers: amount of concurrent worker tasks that dequeue and call sender function. Should be more than window
        - Too many concurrent workers increase CPU usage

    Queue maxsize: max backlog of unsent jobs from the queue. Whel full, either blocks the sending, or drops items from queue
        - increased latency as messages are stored in back of big queue
    """

    def __init__(
        self,
        sender,
        window=16,
        workers=16,
        queue_maxsize=64,
        log_file=None,
        coap_context=None,
    ):
        self.sender = sender
        self.sem = asyncio.Semaphore(window)
        self.queue = asyncio.Queue(maxsize=queue_maxsize)
        self.workers = workers
        self.tasks = []

        self.submitted = 0  # enqueued items
        self.finished = 0  # sender completed (success or error)
        self.in_flight = 0  # awaiting ACK
        self.successful = 0
        self.unknown = 0
        self.error = 0

        self.can_recv = 0
        self.stat_can_recv = self.can_recv

        self.stat = time.monotonic()
        self.stat_submitted = self.submitted
        self.stat_finished = self.finished
        self.can_read_rate = 0.0
        self.ack_rate = 0.0
        self.last_message_id = None

        self.local_pub = None
        self.log_file = open(log_file, "w")

        self.coap_context = coap_context

    def update_stats(self):
        now = time.monotonic()

        dt = now - self.stat

        if dt < 0.5:  # update every 0.5 second
            return

        s = self.can_recv
        f = self.finished

        self.can_read_rate = (s - self.stat_can_recv) / dt
        self.ack_rate = (f - self.stat_finished) / dt

        self.stat = now
        self.stat_can_recv = s
        self.stat_finished = f

    async def start(self):
        if self.tasks:
            return
        for _ in range(self.workers):
            self.tasks.append(asyncio.create_task(self.worker()))

    async def start_coap(self):
        if self.tasks:
            return
        for _ in range(self.workers):
            self.tasks.append(asyncio.create_task(self.coap_worker()))

    async def submit(self, **kwargs):
        self.can_recv += 1
        await self.queue.put(kwargs)

    async def stop(self):
        await self.queue.join()
        for task in self.tasks:
            task.cancel()

        await asyncio.gather(*self.tasks)
        self.tasks.clear()

    async def worker(self):
        try:
            while True:
                kwargs = await self.queue.get()
                self.last_message_id = kwargs.get("msg_id")
                await self.sem.acquire()
                self.in_flight += 1
                try:
                    start_time = time.perf_counter_ns()
                    result_codes = await self.sender(**kwargs)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    print(e)
                finally:
                    stop_time = time.perf_counter_ns()
                    diff_ms = (stop_time - start_time) / 1_000_000
                    self.in_flight -= 1
                    self.finished += 1
                    for result in result_codes:
                        if result == "SUBMITTED":
                            self.submitted += 1
                        elif result == "UNKNOWN":
                            self.unknown += 1
                        elif result == "SUCCESSFUL":
                            self.successful += 1
                        elif result == "ERROR":
                            self.error += 1
                    self.update_stats()

                    stats = self.get_stats()
                    stats["msg_id"] = kwargs.get("msg_id")
                    stats["local_pub"] = diff_ms
                    self.log_file.write(json.dumps(stats) + "\n")
                    self.sem.release()
                    self.queue.task_done()
        except asyncio.CancelledError:
            return

    async def coap_sender(self, kwargs):
        try:
            start_time = time.perf_counter_ns()
            result_codes = await self.sender(**kwargs)
        except Exception as e:
            print(e)
        finally:
            self.queue.task_done()
            stop_time = time.perf_counter_ns()
            diff_ms = (stop_time - start_time) / 1_000_000
            self.local_pub = diff_ms
            self.finished += 1
            for result in result_codes:
                if result == "SUBMITTED":
                    self.submitted += 1
                elif result == "UNKNOWN":
                    self.unknown += 1
                elif result == "SUCCESSFUL":
                    self.successful += 1
                elif result == "ERROR":
                    self.error += 1
            self.update_stats()

            stats = self.get_stats()
            stats["msg_id"] = kwargs.get("msg_id")
            stats["local_pub"] = diff_ms
            self.log_file.write(json.dumps(stats) + "\n")

    async def coap_worker(self):
        try:
            while True:
                kwargs = await self.queue.get()
                self.last_message_id = kwargs.get("msg_id")
                await asyncio.create_task(self.coap_sender(kwargs))

        except asyncio.CancelledError:
            return

    async def shutdown(self, mode="default"):
        shutdowned_messages = []
        last_signal = None

        while self.queue.qsize() > 0:
            await asyncio.sleep(0.1)
            self.update_stats()

            stats = self.get_stats()
            current_signal = stats["msg_id"]

            if current_signal != last_signal:
                shutdowned_messages.append(stats)
                last_signal = current_signal

        await self.queue.join()
        shutdowned_messages.append(self.get_stats())

        curr = self.sem._value
        for _ in range(curr):
            await self.sem.acquire()

        while self.in_flight > 0:
            await asyncio.sleep(0.1)
            self.update_stats()

            stats = self.get_()
            current_signal = stats["msg_id"]

            if current_signal != last_signal:
                shutdowned_messages.append(stats)
                last_signal = current_signal

        for task in self.tasks:
            task.cancel()

        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()

        stop_time = time.time()
        return stop_time, shutdowned_messages

    """
    get the active count directly from aiocoap internals instead of using self.in_flight as aiocoap not using the dispatcher's semaphore for limiting inflight
    """

    def get_coap_inflight(self):
        total = 0
        for tman in self.coap_context.request_interfaces:
            mman = tman.token_interface
            total += len(mman._active_exchanges)

        return total

    def stats(self):

        if self.coap_context is not None:
            current_in_flight = self.get_coap_inflight()

        else:
            current_in_flight = self.in_flight

        print("queue size:", self.queue.qsize())
        print("inflight:", current_in_flight)
        print("finished:", self.finished)
        print("submitted:", self.submitted)
        print("unknown:", self.unknown)
        print("successful:", self.successful)
        print("error:", self.error)

    def get_stats(self):
        ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]

        if self.coap_context is not None:
            current_in_flight = self.get_coap_inflight()
        else:
            current_in_flight = self.in_flight

        return {
            "msg_id": self.last_message_id,
            "time": time.time(),
            "time_date": ts,
            "queue_size": self.queue.qsize(),
            "in_flight": current_in_flight,
            "finished": self.finished,
            "submitted": self.submitted,
            "unknown": self.unknown,
            "successful": self.successful,
            "error": self.error,
            "can_read_rate": round(self.can_read_rate),
            "ack_rate": round(self.ack_rate),
            "local_pub": self.local_pub if self.local_pub else None,
        }
