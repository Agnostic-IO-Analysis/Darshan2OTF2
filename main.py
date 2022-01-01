
import subprocess
import otf2
import darshan
import math


class Event:

    def __init__(self, action, offset, size, start_time, end_time, file_id):
        self.action = action
        self.offset = offset
        self.size = size
        self.start_time = start_time
        self.end_time = end_time
        self.file_id = file_id

    @classmethod
    def get_event_from_dict(cls, action, d, file_id):
        return cls(action, d["offset"], d["length"], d["start_time"], d["end_time"], file_id)

    def __repr__(self):
        return f"{self.action}: time: [{self.start_time} - {self.end_time}], offset: {self.offset}, size: {self.size}"

    def get_start_time_ticks(self, timer_resolution):
        return math.ceil(self.start_time*timer_resolution)

    def get_end_time_ticks(self, timer_resolution):
        return math.ceil(self.end_time*timer_resolution)


def main():
    r = darshan.DarshanReport("sample.darshan", read_all=True, dtype="numpy")
    locations = {}
    location_groups = {}
    TIMER_RESOLUTION = int(1e9)

    with otf2.writer.open("test_trace", timer_resolution=TIMER_RESOLUTION) as trace:

        root_node = trace.definitions.system_tree_node("root node")
        system_tree_node = trace.definitions.system_tree_node("myHost", parent=root_node)
        paradigm_posix = trace.definitions.io_paradigm(identification="POSIX",
                                                       name="POSIX I/O",
                                                       io_paradigm_class=otf2.IoParadigmClass.SERIAL,
                                                       io_paradigm_flags=otf2.IoParadigmFlag.NONE)

        # define functions (source_file is only used so function group is not unknown)
        read_func = trace.definitions.region("read", source_file="POSIX I/O", region_role=otf2.RegionRole.FILE_IO)
        write_func = trace.definitions.region("write", source_file="POSIX I/O", region_role=otf2.RegionRole.FILE_IO)
        functions = {"read": read_func, "write": write_func}
        operation_modes = {"read": otf2.IoOperationMode(0), "write": otf2.IoOperationMode(1)}

        # create file definitions
        io_handles = {}
        for file_id, file_name in dict(r.name_records).items():
            if file_name in ["<STDIN>", "<STDOUT>", "<STDERR>"]:
                io_file = None
                handle_name = file_name
                handle_flag = otf2.IoHandleFlag(1)
            else:
                io_file = trace.definitions.io_regular_file(file_name, scope=system_tree_node)
                handle_name = ""
                # should all handles be made pre-created ?
                handle_flag = otf2.IoHandleFlag(0)
            io_handle = trace.definitions.io_handle(file=io_file,
                                                    name=handle_name,
                                                    io_paradigm=paradigm_posix,
                                                    io_handle_flags=handle_flag)
            io_handles.update({file_id: io_handle})

        # # unnecessary ?
        # create base group, all locations will be attached to
        # location_group = trace.definitions.location_group("Master Process",
        #                                                   system_tree_parent=system_tree_node,
        #                                                   location_group_type=otf2.LocationGroupType.PROCESS)

        i = 0
        for batch in r.records["DXT_POSIX"]:
            if i > 50:
                break
            i += 1
            events = []

            # specifies file the process is working on
            file_id = batch["id"]
            # rank is a process in the program traced, treat as location group for now
            rank = batch["rank"]

            # # unnecessary ?
            # # add location for rank
            # location = trace.definitions.location(f"rank {rank}", group=location_group)
            # locations.update({rank: location})

            location_group = trace.definitions.location_group(f"rank {rank}",
                                                              system_tree_parent=system_tree_node,
                                                              location_group_type=otf2.LocationGroupType.PROCESS)

            location_groups.update({rank: location_group})

            # gather events
            for item in batch["write_segments"]:
                events.append(Event.get_event_from_dict("write", item, file_id))
            for item in batch["read_segments"]:
                events.append(Event.get_event_from_dict("read", item, file_id))

            # sorting events within a batch is fine because no operations overlap
            events.sort(key=lambda x: x.start_time)

            writer = trace.event_writer(f"rank {rank}", group=location_group)

            # write events

            for event in events:
                writer.enter(event.get_start_time_ticks(TIMER_RESOLUTION),
                             functions.get(event.action))

                writer.io_operation_begin(time=event.get_start_time_ticks(TIMER_RESOLUTION),
                                          handle=io_handles.get(event.file_id),
                                          mode=operation_modes.get(event.action),
                                          operation_flags=otf2.IoOperationFlag.NONE,
                                          bytes_request=event.size,
                                          matching_id=0)

                writer.io_operation_complete(time=event.get_end_time_ticks(TIMER_RESOLUTION),
                                             handle=io_handles.get(event.file_id),
                                             bytes_result=event.size,
                                             matching_id=0)

                writer.leave(event.get_end_time_ticks(TIMER_RESOLUTION),
                             functions.get(event.action))


if __name__ == '__main__':

    subprocess.run(["rm", "-rf", "./test_trace"])
    main()
    # with otf2.reader.open("./test_trace/traces.otf2") as trace:
    #     for l in trace.definitions.regions:
    #         print(l)
    #
    #
