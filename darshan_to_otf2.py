import otf2
import argparse
import subprocess
import os

import util


def write_oft2_trace(fp_output, timer_res, stats):

    with otf2.writer.open(fp_output, timer_resolution=timer_res) as trace:

        root_node = trace.definitions.system_tree_node(stats["job"]["root_node"])
        system_tree_nodes = {name: trace.definitions.system_tree_node(name, parent=root_node) for name in util.get_hostnames(stats)}
        paradigms = {}

        # might break if empty report
        counter_keys = util.get_counter_keys(stats)
        metric_members = {key: trace.definitions.metric_member(name=key, metric_mode=otf2.MetricMode.ABSOLUTE_LAST) for key in counter_keys}
        metric_classes = {key: trace.definitions.metric_class(members=(metric_members.get(key),)) for key in counter_keys}
        metric_instances = {}

        if "posix" in stats["paradigms"].keys():
            paradigms["posix"] = trace.definitions.io_paradigm(identification="POSIX",
                                                               name="POSIX I/O",
                                                               io_paradigm_class=otf2.IoParadigmClass.SERIAL,
                                                               io_paradigm_flags=otf2.IoParadigmFlag.NONE)

        if "mpi" in stats["paradigms"].keys():
            paradigms["mpi"] = trace.definitions.io_paradigm(identification="MPI",
                                                             name="MPI I/0",
                                                             io_paradigm_class=otf2.IoParadigmClass.PARALLEL,
                                                             io_paradigm_flags=otf2.IoParadigmFlag.NONE)

        regions = {("posix", "read"): trace.definitions.region("posix_read",
                                                               source_file="POSIX I/O",
                                                               region_role=otf2.RegionRole.FILE_IO),
                   ("posix", "write"): trace.definitions.region("posix_write",
                                                                source_file="POSIX I/O",
                                                                region_role=otf2.RegionRole.FILE_IO),

                   ("mpi", "read"): trace.definitions.region("mpi_read",
                                                             source_file="MPI I/O",
                                                             region_role=otf2.RegionRole.FILE_IO),
                   ("mpi", "write"): trace.definitions.region("mpi_write",
                                                              source_file="MPI I/O",
                                                              region_role=otf2.RegionRole.FILE_IO)}

        files = util.get_files(stats)

        io_files = {file_id: trace.definitions.io_regular_file(file_name,
                                                               scope=system_tree_nodes.get(file_hostname)) for
                    file_id, file_name, file_hostname in files if
                    file_name not in ["<STDIN>", "<STDOUT>", "<STDERR>"]}

        io_handles = {}
        for file_id, file_name, file_hostname in files:
            if file_name in ["<STDIN>", "<STDOUT>", "<STDERR>"]:
                handle_name = file_name
                handle_flag = otf2.definitions.enums.IoHandleFlag.PRE_CREATED
            else:
                handle_name = ""
                handle_flag = otf2.IoHandleFlag.NONE

            for paradigm in stats["paradigms"]:
                io_handles[(paradigm, file_id)] = trace.definitions.io_handle(file=io_files.get(file_id),
                                                                              name=handle_name,
                                                                              io_paradigm=paradigms.get(paradigm),
                                                                              io_handle_flags=handle_flag)
        ranks_hostnames = util.get_ranks_hostnames(stats)
        location_groups = {f"rank {rank_id}": trace.definitions.location_group(f"rank {rank_id}", system_tree_parent=system_tree_nodes.get(rank_hostname)) for rank_id, rank_hostname in ranks_hostnames}
        locations = {f"rank {rank_id}": trace.definitions.location("Master Thread", group=location_groups.get(f"rank {rank_id}")) for rank_id in [item[0] for item in ranks_hostnames]}

        #for rank_id, rank_stats in stats["ranks"].items():

        for paradigm in stats["paradigms"]:
            for file_id, file_name in stats["paradigms"][paradigm]["files"]:
                element = stats["paradigms"][paradigm]["files"][(file_id, file_name)]

                events = element["read_events"] + element["write_events"]
                events.sort(key=lambda x: x.start_time)

                #writer = trace.event_writer(f"Master Thread", group=locations.get(f"rank {rank_id}"))
                writer = trace.event_writer_from_location(locations.get(f"rank {element['rank']}"))
                t_last = 0
                if len(events) > 0:
                    t_start = events[0].get_start_time_ticks(timer_res)
                for event in events:
                    io_mode = otf2.IoOperationMode.WRITE if event.action == "write" else otf2.IoOperationMode.READ

                    writer.enter(event.get_start_time_ticks(timer_res)-t_start,
                                 regions.get((event.paradigm, event.action)))

                    writer.io_operation_begin(time=event.get_start_time_ticks(timer_res)-t_start,
                                              handle=io_handles.get((event.paradigm, event.file_id)),
                                              mode=io_mode,
                                              operation_flags=otf2.IoOperationFlag.NONE,
                                              bytes_request=event.size,
                                              matching_id=0)

                    writer.io_operation_complete(time=event.get_end_time_ticks(timer_res)-t_start,
                                                 handle=io_handles.get((event.paradigm, event.file_id)),
                                                 bytes_result=event.size,
                                                 matching_id=0)

                    writer.leave(event.get_end_time_ticks(timer_res)-t_start,
                                 regions.get((event.paradigm, event.action)))

                    t_last = event.get_end_time_ticks(timer_res)-t_start

                # metrics

                for k, v in element["counters"].items():

                    location = locations.get(f"rank {element['rank']}")

                    if v == 0:
                        continue
                    if metric_instances.get((metric_classes.get(k), location)) is None:

                        metric_instance = trace.definitions.metric_instance(metric_class=metric_classes.get(k),
                                                                            recorder=location, scope=location)
                        metric_instances[(metric_classes.get(k), location)] = metric_instance

                    metric_instance = metric_instances[(metric_classes.get(k), location)]
                    try:
                        writer.metric(time=t_last, metric=metric_instance, values=v)
                    except:
                        print(v)
                    t_last += 1


def main():

    ap = argparse.ArgumentParser()
    ap.add_argument("file", type=str, help="file path to the darshan trace file")
    ap.add_argument("-o", "--output", type=str, help="specifies different output path, default is ./trace_out")
    ap.add_argument("-t", "--timer", type=int, help="sets timer resolution, default is 1e9")
    args = ap.parse_args()

    fp_out = "./trace_out" if args.output is None else args.output
    timer_res = int(1e9) if args.timer is None else args.timer

    if os.path.isdir(fp_out):
        subprocess.run(["rm", "-rf", fp_out])

    stats = util.gather_stats_from_darshan(args.file)
    write_oft2_trace(fp_out, timer_res, stats)


if __name__ == '__main__':
    main()
