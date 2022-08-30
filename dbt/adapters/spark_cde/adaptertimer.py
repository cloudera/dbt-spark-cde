import time
from dbt.events import AdapterLogger

logger = AdapterLogger("Spark")


class AdapterTimer:
    _timers = []

    def get_timer(self, timer_name):
        return list(filter(lambda timer: timer["name"] == timer_name, self._timers))

    def add_timer(self, timer):
        self._timers.append(timer)

    def remove_timer(self, timer):
        self._timers.remove(timer)

    def start_timer(self, timer_name):
        prev_timer = self.get_timer(timer_name)
        has_timer = len(prev_timer) == 1
        if has_timer:
            prev_timer = prev_timer[0]
            prev_timer["start_time"] = time.time()
            prev_timer["end_time"] = prev_timer["start_time"]
            return prev_timer

        new_timer = {"name": timer_name, "start_time": time.time()}
        new_timer["end_time"] = new_timer["start_time"]
        new_timer["elapsed_time"] = 0
        self.add_timer(new_timer)
        return new_timer

    def end_timer(self, timer_name):
        prev_timer = self.get_timer(timer_name)
        has_timer = len(prev_timer) == 1
        if has_timer:
            prev_timer = prev_timer[0]
            prev_timer["end_time"] = time.time()
            prev_timer["elapsed_time"] = (
                prev_timer["end_time"] - prev_timer["start_time"]
            )
            return prev_timer["elapsed_time"]

        print("Timer ", timer_name, " not found")
        return None

    def log_summary(self):
        for timer in self._timers:
            logger.debug("{:<20}{:10.2f}".format(timer["name"], timer["elapsed_time"]))
