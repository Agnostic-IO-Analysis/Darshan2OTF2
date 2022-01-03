import otf2
import darshan
import argparse
import subprocess
import os
from event import Event


def write_definitions(fp_output, timer_res, report):

    with otf2.writer.open(fp_output, timer_resolution=timer_res) as trace:

        root_node = trace.definitions.system_tree_node("root node")
        system_tree_node = trace.definitions.system_tree_node("myHost", parent=root_node)

        # define paradigms

        paradigm_posix = trace.definitions.io_paradigm(identification="POSIX",
                                                       name="POSIX I/O",
                                                       io_paradigm_class=otf2.IoParadigmClass.SERIAL,
                                                       io_paradigm_flags=otf2.IoParadigmFlag.NONE)

        paradigm_mpi = trace.definitions.io_paradigm(identification="MPI",
                                                     name="MPI I/O",
                                                     io_paradigm_class=otf2.IoParadigmClass.PARALLEL,
                                                     io_paradigm_flags=otf2.IoParadigmFlag.NONE)

        paradigms = {"posix": paradigm_posix, "mpi": paradigm_mpi}

        # define regions (source_file is only used so function group is not unknown)

        functions = {("posix", "read"): trace.definitions.region("posix_read",
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

        # create file handles
        io_handles = {}
        for file_id, file_name in dict(report.name_records).items():
            if file_name in ["<STDIN>", "<STDOUT>", "<STDERR>"]:
                io_file = None
                handle_name = file_name
                handle_flag = otf2.definitions.enums.IoHandleFlag.NONE
            else:
                io_file = trace.definitions.io_regular_file(file_name, scope=system_tree_node)
                handle_name = ""
                # should all handles be made pre-created ?
                handle_flag = otf2.IoHandleFlag.PRE_CREATED

            # create a different handle per paradigm
            for paradigm in ["posix", "mpi"]:
                io_handle = trace.definitions.io_handle(file=io_file,
                                                        name=handle_name,
                                                        io_paradigm=paradigms.get(paradigm),
                                                        io_handle_flags=handle_flag)

                io_handles.update({(paradigm, file_id): io_handle})

        location_groups = {"posix": trace.definitions.location_group("POSIX",
                                                                     system_tree_parent=system_tree_node),
                           "mpi": trace.definitions.location_group("MPI",
                                                                   system_tree_parent=system_tree_node)}
    return functions, io_handles, location_groups


def write_events(fp_tmp, fp_out, timer_res, report, functions, io_handles, location_groups):

    locations = {}

    with otf2.reader.open(fp_tmp) as tmp_trace:
        with otf2.writer.open(fp_out, definitions=tmp_trace.definitions) as trace:

            for section, paradigm in [("DXT_POSIX", "posix"), ("DXT_MPIIO", "mpi")]:

                i = 0
                for batch in report.records[section]:
                    if i > 10:
                        break
                    i += 1
                    events = []

                    # specifies file the process is working on
                    file_id = batch["id"]
                    # rank is a process in the program traced, treat as location group for now
                    rank = batch["rank"]

                    # creates a location for each rank in the respecting location group (paradigm)
                    location = trace.definitions.location(f"{paradigm}_rank {rank}", group=location_groups.get(paradigm))
                    locations.update({rank: location})

                    # gather events
                    for item in batch["write_segments"]:
                        events.append(Event.get_event_from_dict(f"write", item, paradigm, file_id))
                    for item in batch["read_segments"]:
                        events.append(Event.get_event_from_dict(f"read", item, paradigm, file_id))

                    # sorting events within a batch is fine because no operations overlap
                    events.sort(key=lambda x: x.start_time)

                    writer = trace.event_writer(f"{paradigm}_rank {rank}", group=location_groups.get(paradigm))

                    # write events

                    for event in events:
                        io_mode = otf2.IoOperationMode.WRITE if event.action == "write" else otf2.IoOperationMode.READ

                        writer.enter(event.get_start_time_ticks(timer_res),
                                     functions.get((paradigm, event.action)))

                        writer.io_operation_begin(time=event.get_start_time_ticks(timer_res),
                                                  handle=io_handles.get((paradigm, event.file_id)),
                                                  mode=io_mode,
                                                  operation_flags=otf2.IoOperationFlag.NONE,
                                                  bytes_request=event.size,
                                                  matching_id=0)

                        writer.io_operation_complete(time=event.get_end_time_ticks(timer_res),
                                                     handle=io_handles.get((paradigm, event.file_id)),
                                                     bytes_result=event.size,
                                                     matching_id=0)

                        writer.leave(event.get_end_time_ticks(timer_res),
                                     functions.get((paradigm, event.action)))


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

    report = darshan.DarshanReport(args.file, read_all=True, dtype="numpy")

    functions, io_handles, location_groups = write_definitions("./tmp", timer_res, report)
    write_events("./tmp/traces.otf2", fp_out, timer_res, report, functions, io_handles, location_groups)
    subprocess.run(["rm", "-rf", "./tmp"])


if __name__ == '__main__':
    main()
