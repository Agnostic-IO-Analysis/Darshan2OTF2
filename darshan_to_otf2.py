import otf2
import argparse
import subprocess
import os
from util import gather_stats_from_darshan


def write_oft2_trace(fp_output, timer_res, stats):

    with otf2.writer.open(fp_output, timer_resolution=timer_res) as trace:

        root_node = trace.definitions.system_tree_node(stats["job"]["root_node"])
        system_tree_nodes = {name: trace.definitions.system_tree_node(name, parent=root_node) for name in stats["hostnames"]}
        paradigms = {}

        if "posix" in stats["paradigms"]:
            paradigms["posix"] = trace.definitions.io_paradigm(identification="POSIX",
                                                               name="POSIX I/O",
                                                               io_paradigm_class=otf2.IoParadigmClass.SERIAL,
                                                               io_paradigm_flags=otf2.IoParadigmFlag.NONE)

        if "mpi" in stats["paradigms"]:
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

        io_files = {file_id: trace.definitions.io_regular_file(file_atr["name"],
                                                               scope=system_tree_nodes.get(file_atr["hostname"])) for
                    file_id, file_atr in stats["files"].items() if
                    file_atr["name"] not in ["<STDIN>", "<STDOUT>", "<STDERR>"]}

        io_handles = {}
        for file_id, file_atr in stats["files"].items():
            if file_atr["name"] in ["<STDIN>", "<STDOUT>", "<STDERR>"]:
                handle_name = file_atr["name"]
                handle_flag = otf2.definitions.enums.IoHandleFlag.PRE_CREATED
            else:
                handle_name = ""
                handle_flag = otf2.IoHandleFlag.NONE

            for paradigm in stats["paradigms"]:
                io_handles[(paradigm, file_id)] = trace.definitions.io_handle(file=io_files.get(file_id),
                                                                              name=handle_name,
                                                                              io_paradigm=paradigms.get(paradigm),
                                                                              io_handle_flags=handle_flag)

        location_groups = {f"rank {rank_id}": trace.definitions.location_group(f"rank {rank_id}", system_tree_parent=system_tree_nodes.get(rank_atr["hostname"])) for rank_id, rank_atr in stats["ranks"].items()}
        locations = {f"rank {rank_id}": trace.definitions.location("Master Thread", group=location_groups.get(f"rank {rank_id}")) for rank_id in stats["ranks"].keys()}

        for rank_id, rank_stats in stats["ranks"].items():
            events = rank_stats["read_events"] + rank_stats["write_events"]
            events.sort(key=lambda x: x.start_time)

            #writer = trace.event_writer(f"Master Thread", group=locations.get(f"rank {rank_id}"))
            writer = trace.event_writer_from_location(locations.get(f"rank {rank_id}"))

            for event in events:
                io_mode = otf2.IoOperationMode.WRITE if event.action == "write" else otf2.IoOperationMode.READ

                writer.enter(event.get_start_time_ticks(timer_res),
                             regions.get((event.paradigm, event.action)))

                writer.io_operation_begin(time=event.get_start_time_ticks(timer_res),
                                          handle=io_handles.get((event.paradigm, event.file_id)),
                                          mode=io_mode,
                                          operation_flags=otf2.IoOperationFlag.NONE,
                                          bytes_request=event.size,
                                          matching_id=0)

                writer.io_operation_complete(time=event.get_end_time_ticks(timer_res),
                                             handle=io_handles.get((event.paradigm, event.file_id)),
                                             bytes_result=event.size,
                                             matching_id=0)

                writer.leave(event.get_end_time_ticks(timer_res),
                             regions.get((event.paradigm, event.action)))


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

    stats = gather_stats_from_darshan(args.file)
    write_oft2_trace(fp_out, timer_res, stats)


if __name__ == '__main__':
    main()
