import math


class Event:

    def __init__(self, action, offset, size, start_time, end_time, paradigm, file_name, location, hostname):
        self.action = action
        self.offset = offset
        self.size = size
        self.start_time = start_time
        self.end_time = end_time
        self.paradigm = paradigm
        self.file_name = file_name
        self.location = location # rank
        self.hostname = hostname

    @classmethod
    def get_event_from_dict(cls, action, d, paradigm, file_name, location, hostname):
        return cls(action, d["offset"], d["length"], d["start_time"], d["end_time"], paradigm, file_name, location, hostname)

    def __repr__(self):
        return f"{self.file_name} ({self.paradigm}_{self.action}): time: [{self.start_time} - {self.end_time}], " \
               f"offset: {self.offset}, size: {self.size}"

    def get_start_time_ticks(self, timer_resolution):
        return math.ceil(self.start_time*timer_resolution)

    def get_end_time_ticks(self, timer_resolution):
        return math.ceil(self.end_time*timer_resolution)
