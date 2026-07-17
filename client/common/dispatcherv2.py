import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Maps a result code returned by `sender` to the counter attribute it bumps.
_RESULT_ATTR = {
    "SUBMITTED": "submitted",
    "UNKNOWN": "unknown",
    "SUCCESSFUL": "successful",
    "ERROR": "error",
}


class Dispatcher:
    """
    Generic helper class for handling concurrency in ACKS.
    When publishing CAN data, there are 3 important settings:

    Semaphore window: amount of in-flight messages awaiting ACK. If all slots
        are full, new sends need to wait for a slot to be freed.
        - Too big a window overloads the server, increasing tail latency.

    Workers: amount of concurrent worker tasks that dequeue and call the
        sender function. Should be >= window.
        - Too many concurrent workers increases CPU usage for no benefit,
          since they'll just be waiting on the semaphore anyway.

    Queue maxsize: max backlog of unsent jobs. When full, `submit()` blocks.
        - A big queue increases latency (messages sit at the back longer).
    """

    def __init__(
        self,
        sender: Callable[..., Awaitable[Iterable[str]]],
        window: int = 16,
        workers: int = 16,
        queue_maxsize: int = 64,
        log_file: Optional[str] = None,
        coap_context: Any = None,
    ):
        self.sender = sender
        self.sem = asyncio.Semaphore(window)
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)
        self.workers = workers
        self.tasks: List[asyncio.Task] = []

        # message counters
        self.submitted = 0  # sender reported "SUBMITTED"
        self.finished = 0  # sender call completed (success or error)
        self.in_flight = 0  # awaiting ACK
        self.successful = 0
        self.unknown = 0
        self.error = 0

        self.can_recv = 0
        self._stat_can_recv = self.can_recv
        self._stat_finished = self.finished
        self._stat_time = time.monotonic()

        self.can_read_rate = 0.0
        self.ack_rate = 0.0
        self.last_message_id = None
        self.local_pub: Optional[float] = None

        # Only open a log file if one was actually requested.
        self._log_file = open(log_file, "w") if log_file else None

        self.coap_context = coap_context

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #

    async def start(self):
        """
        Start worker tasks. Behavior is picked automatically from whether
        `coap_context` was passed in: plain sends are semaphore/in_flight
        gated, CoAP sends are fired unbounded since aiocoap tracks its own
        concurrency (see get_coap_inflight).
        """
        if self.tasks:
            return
        self.tasks = [asyncio.create_task(self._worker()) for _ in range(self.workers)]

    async def start_coap(self):
        """Deprecated alias for `start()` - mode is now auto-detected."""
        await self.start()

    async def submit(self, **kwargs):
        self.can_recv += 1
        await self.queue.put(kwargs)

    async def stop(self):
        """Graceful stop: drain the queue, then cancel workers."""
        await self.queue.join()
        await self._cancel_tasks()
        self._close_log()

    async def shutdown(self, mode: str = "default") -> Tuple[float, List[dict]]:
        """
        Wait for the queue to drain and any in-flight sends to finish,
        recording a stats snapshot whenever the "current message" changes,
        then cancel the workers.
        """
        shutdown_log: List[dict] = []
        last_signal = None

        async def poll_until_false(still_running: Callable[[], bool]):
            nonlocal last_signal
            while still_running():
                await asyncio.sleep(0.1)
                self.update_stats()
                stats = self.get_stats()
                if stats["msg_id"] != last_signal:
                    shutdown_log.append(stats)
                    last_signal = stats["msg_id"]

        await poll_until_false(lambda: self.queue.qsize() > 0)
        await self.queue.join()
        shutdown_log.append(self.get_stats())

        # Drain the semaphore so nothing new can start, then wait for
        # whatever is already in flight to finish.
        held = self.sem._value
        for _ in range(held):
            await self.sem.acquire()

        await poll_until_false(lambda: self.in_flight > 0)

        await self._cancel_tasks()
        self._close_log()

        return time.time(), shutdown_log

    async def _cancel_tasks(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()

    def _close_log(self):
        if self._log_file:
            self._log_file.close()
            self._log_file = None

    # ------------------------------------------------------------------ #
    # workers
    # ------------------------------------------------------------------ #

    async def _run_sender(self, kwargs) -> Tuple[Iterable[str], float]:
        """
        Call the sender and always return (result_codes, elapsed_ms) - even
        if the sender raises, so callers never have to guard against an
        unset result_codes.
        """
        start_time = time.perf_counter_ns()
        try:
            result_codes = await self.sender(**kwargs)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("sender failed for msg_id=%s", kwargs.get("msg_id"))
            result_codes = ("ERROR",)
        stop_time = time.perf_counter_ns()
        return result_codes, (stop_time - start_time) / 1_000_000

    def _tally(self, result_codes: Iterable[str]):
        for result in result_codes:
            attr = _RESULT_ATTR.get(result)
            if attr:
                setattr(self, attr, getattr(self, attr) + 1)

    def _record(self, kwargs, diff_ms: float):
        self.finished += 1
        self.update_stats()
        if self._log_file:
            stats = self.get_stats()
            stats["msg_id"] = kwargs.get("msg_id")
            stats["local_pub"] = diff_ms
            self._log_file.write(json.dumps(stats) + "\n")

    async def _process(self, kwargs):
        """Call the sender for one item, tally/log the result, mark it done."""
        try:
            result_codes, diff_ms = await self._run_sender(kwargs)
            self._tally(result_codes)
            self.local_pub = diff_ms
            self._record(kwargs, diff_ms)
        finally:
            self.queue.task_done()

    async def _worker(self):
        """
        Single worker loop for all protocols. The only thing that differs
        between them is whether an item is gated by our own semaphore/
        in_flight counter (plain sends) or fired off unbounded because the
        underlying transport already manages its own concurrency (CoAP).
        """
        coap_mode = self.coap_context is not None
        try:
            while True:
                kwargs = await self.queue.get()
                self.last_message_id = kwargs.get("msg_id")

                if coap_mode:
                    # aiocoap tracks its own in-flight exchanges, so don't
                    # gate task creation - just fan requests out.
                    asyncio.create_task(self._process(kwargs))
                    continue

                await self.sem.acquire()
                self.in_flight += 1
                try:
                    await self._process(kwargs)
                finally:
                    self.in_flight -= 1
                    self.sem.release()
        except asyncio.CancelledError:
            return

    # ------------------------------------------------------------------ #
    # stats
    # ------------------------------------------------------------------ #

    def update_stats(self):
        now = time.monotonic()
        dt = now - self._stat_time
        if dt < 0.5:  # update at most every 0.5s
            return

        self.can_read_rate = (self.can_recv - self._stat_can_recv) / dt
        self.ack_rate = (self.finished - self._stat_finished) / dt

        self._stat_time = now
        self._stat_can_recv = self.can_recv
        self._stat_finished = self.finished

    def get_coap_inflight(self) -> int:
        """
        aiocoap doesn't use our semaphore to limit in-flight requests, so pull
        the real count from its internals instead of trusting self.in_flight.
        """
        total = 0
        for tman in self.coap_context.request_interfaces:
            mman = tman.token_interface
            total += len(mman._active_exchanges)
        return total

    @property
    def _current_in_flight(self) -> int:
        if self.coap_context is not None:
            return self.get_coap_inflight()
        return self.in_flight

    def get_stats(self) -> Dict[str, Any]:
        return {
            "msg_id": self.last_message_id,
            "time": time.time(),
            "time_date": datetime.now().strftime("%H:%M:%S.%f")[:-3],
            "queue_size": self.queue.qsize(),
            "in_flight": self._current_in_flight,
            "finished": self.finished,
            "submitted": self.submitted,
            "unknown": self.unknown,
            "successful": self.successful,
            "error": self.error,
            "can_read_rate": round(self.can_read_rate),
            "ack_rate": round(self.ack_rate),
            "local_pub": self.local_pub,
        }

    def stats(self):
        print("queue size:", self.queue.qsize())
        print("inflight:", self._current_in_flight)
        print("finished:", self.finished)
        print("submitted:", self.submitted)
        print("unknown:", self.unknown)
        print("successful:", self.successful)
        print("error:", self.error)
