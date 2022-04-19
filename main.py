import darshan
import otf2
import math
import os
import subprocess


def combine_dicts(d1, d2):
    return dict(zip((d1 | d2).keys(), [d1.get(k, 0) + d2.get(k, 0) for k in (d1 | d2).keys()]))


def main(fp_in, fp_out, timer_res):
    report = darshan.DarshanReport(fp_in, read_all=True, dtype="dict")

    paradigms = {}
    regions = {}
    system_tree_nodes = {}
    io_regular_files = {}
    io_handles = {}
    location_groups = {}
    locations = {}
    metric_locations = {}

    segments_per_location = {}

    io_paradigm_classes = {"posix": otf2.IoParadigmClass.SERIAL,
                           "mpi": otf2.IoParadigmClass.PARALLEL}

    io_operation_modes = {"read": otf2.IoOperationMode.READ,
                          "write": otf2.IoOperationMode.WRITE}

    with otf2.writer.open(fp_out, timer_resolution=timer_res) as trace:

        # create basic stuff
        root_node = trace.definitions.system_tree_node("root_node")

        for section, paradigm in [("DXT_POSIX", "posix"), ("DXT_MPIIO", "mpi")]:
            try:
                for batch in report.records[section]:
                    bid, rank, hostname, write_count, read_count, write_segments, read_segments = batch["id"], batch["rank"],\
                                                                                                  batch["hostname"],\
                                                                                                  batch["write_count"],\
                                                                                                  batch["read_count"],\
                                                                                                  batch["write_segments"], \
                                                                                                  batch["read_segments"]

                    if segments_per_location.get(rank) is None:
                        segments_per_location.update({rank: []})

                    # create paradigm and read and write region for that paradigm
                    if paradigms.get(paradigm) is None:
                        paradigms.update({paradigm: trace.definitions.io_paradigm(identification=paradigm.upper(),
                                                                                  name=paradigm.upper() + " I/O",
                                                                                  io_paradigm_class=io_paradigm_classes.get(paradigm),
                                                                                  io_paradigm_flags=otf2.IoParadigmFlag.NONE)})
                        for mode in ["read", "write"]:
                            # ToDo source file correct ?
                            regions.update({(paradigm, mode): trace.definitions.region(f"{paradigm}_{mode}",
                                                                                       source_file=paradigm.upper() + " I/O",
                                                                                       region_role=otf2.RegionRole.FILE_IO)})

                    # create system tree node for host name
                    if system_tree_nodes.get(hostname) is None:
                        system_tree_nodes.update({hostname: trace.definitions.system_tree_node(hostname, parent=root_node)})

                    # collect segments
                    write_segments = [("write", segment) for segment in write_segments]
                    read_segments = [("read", segment) for segment in read_segments]
                    segments = write_segments + read_segments
                    segments_per_location[rank] += segments

                for rank, segments in segments_per_location.items():

                    # write segments
                    t_last = 0
                    segments = sorted(segments, key=lambda x: x[1]["start_time"])
                    t_start = math.ceil(segments[0][1]["start_time"]*timer_res)
                    for mode, item in segments:
                        offset, length, start_time, end_time = item["offset"], item["length"], item["start_time"], \
                                                               item["end_time"]

                        # create io_regular_file and handle if not exists

                        file_name = report.name_records.get(bid)
                        if io_regular_files.get(file_name) is None:
                            # if file_name not in ["<STDIN>", "<STDOUT>", "<STDERR>"]:
                            io_regular_files.update({file_name: trace.definitions.io_regular_file(file_name,
                                                                                                  scope=system_tree_nodes.get(
                                                                                                      hostname))})

                            if file_name in ["<STDIN>", "<STDOUT>", "<STDERR>"]:
                                handle_name = file_name
                                handle_flag = otf2.definitions.enums.IoHandleFlag.PRE_CREATED
                            else:
                                handle_name = ""
                                handle_flag = otf2.IoHandleFlag.NONE

                            io_handles.update({(paradigm, file_name): trace.definitions.io_handle(file=io_regular_files.get(file_name),
                                                                            name=handle_name,
                                                                            io_paradigm=paradigms.get(paradigm),
                                                                            io_handle_flags=handle_flag)})

                        # create location and location group if not exists
                        # ToDo hostname is incorrect does this even matter ?
                        if location_groups.get(f"rank {rank}") is None:
                            location_groups.update({f"rank {rank}": trace.definitions.location_group(f"rank {rank}",
                                                                                                     system_tree_parent=system_tree_nodes.get(
                                                                                                         hostname))})
                            locations.update({f"rank {rank}": trace.definitions.location("Master Thread",
                                                                                         group=location_groups.get(
                                                                                             f"rank {rank}"))})
                            metric_locations.update({f"metric_{rank}": trace.definitions.location(f"metric_{rank}", type=otf2.LocationType.METRIC, group=location_groups.get(f"rank {rank}"))})

                        writer = trace.event_writer_from_location(locations.get(f"rank {rank}"))
                        writer.enter(math.ceil(timer_res * start_time) - t_start, regions.get((paradigm, mode)))

                        writer.io_operation_begin(time=math.ceil(timer_res * start_time)-t_start,
                                                  handle=io_handles.get((paradigm, file_name)),
                                                  mode=io_operation_modes.get(mode),
                                                  operation_flags=otf2.IoOperationFlag.NONE,
                                                  bytes_request=length,
                                                  matching_id=0)

                        writer.io_operation_complete(time=math.ceil(timer_res * start_time)-t_start,
                                                     handle=io_handles.get((paradigm, file_name)),
                                                     bytes_result=length,
                                                     matching_id=0)

                        writer.leave(math.ceil(timer_res * end_time)-t_start, regions.get((paradigm, mode)))
                        t_last = math.ceil(timer_res * end_time)

            except KeyError as e:
                print(f"skipping section {e}")

        # metrics (just collected information about the trace, no location, rank, file info etc)
        # ToDO Lustre/StdIO also ?

        # collecting values
        metrics = {}
        loc_refs = [loc._ref for loc in trace.definitions.locations]

        for section in ["POSIX", "MPIIO"]:
            try:
                counters = report.counters[section]["counters"]
                for counter in counters:
                    metrics.update({counter: {loc_ref: 0 for loc_ref in loc_refs}})
            except KeyError as e:
                print(e)

        for section in ["POSIX", "MPIIO"]:
            try:
                for batch in report.records[section]:
                    rank = batch["rank"]
                    bid = batch["id"]
                    counters = batch["counters"]
                    for key, value in counters.items():
                        metrics[key][rank] += value
            except KeyError as e:
                print(f"skipping section {e}")

        # remove zeros
        # metrics = {k: v for k, v in metrics.items() if v != 0}
        metric_region = trace.definitions.region(f"Metric", source_file="Metric", region_role=otf2.RegionRole.FILE_IO)
        metric_members = {}
        for metric_name, data in metrics.items():
            metric_members.update({metric_name: trace.definitions.metric_member(name=metric_name, metric_mode=otf2.MetricMode.ABSOLUTE_LAST)})
        metric_class = trace.definitions.metric_class(members=(tuple(metric_members.keys())))

        for metric_name, data in metrics.items():
            for rank, value in data.items():

                metric_instance = trace.definitions.metric_instance(metric_class=metric_class,
                                                                    recorder=metric_locations.get(f"metric_{rank}"),
                                                                    scope=metric_locations.get(f"metric_{rank}"))
                writer.enter(t_last, metric_region)
                writer.metric(time=t_last+1, metric=metric_instance, values=value)
                writer.leave(t_last+2, metric_region)
                t_last += 3


if os.path.isdir("./trace_out"):
    subprocess.run(["rm", "-rf", "./trace_out"])
main("./traces/qe-lustre1.darshan", "./trace_out", 100000)
