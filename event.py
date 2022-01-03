import math


class Event:

    def __init__(self, action, offset, size, start_time, end_time, paradigm, file_id):
        self.action = action
        self.offset = offset
        self.size = size
        self.start_time = start_time
        self.end_time = end_time
        self.paradigm = paradigm
        self.file_id = file_id

    @classmethod
    def get_event_from_dict(cls, action, d, paradigm, file_id):
        return cls(action, d["offset"], d["length"], d["start_time"], d["end_time"], paradigm, file_id)

    def __repr__(self):
        return f"{self.file_id} ({self.paradigm}_{self.action}): time: [{self.start_time} - {self.end_time}], " \
               f"offset: {self.offset}, size: {self.size}"

    def get_start_time_ticks(self, timer_resolution):
        return math.ceil(self.start_time*timer_resolution)

    def get_end_time_ticks(self, timer_resolution):
        return math.ceil(self.end_time*timer_resolution)
