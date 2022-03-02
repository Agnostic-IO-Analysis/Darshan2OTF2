import darshan
from event import Event


def get_stats_from_darshan(fp):

    report = darshan.DarshanReport(fp, read_all=True, dtype="dict")

    name_records = report.name_records
    events = []
    definitions = {"paradigms": set(), "file_loc_host": set()}
    counters = {"data": {}, "keys": set()}

    for section, paradigm in [("DXT_POSIX", "posix"), ("DXT_MPIIO", "mpi")]:
        try:
            definitions["paradigms"].add(paradigm)
            for batch in report.records[section]:
                hostname = batch["hostname"]
                location = batch["rank"]
                file_id = batch["id"]
                file_name = name_records[file_id]

                definitions["file_loc_host"].add((file_name, location, hostname))

                events += [Event("write", *event.values(), paradigm, file_name=file_name, location=location,
                                 hostname=hostname) for event in batch["write_segments"]]
                events += [Event("read", *event.values(), paradigm, file_name=file_name, location=location,
                                 hostname=hostname) for event in batch["read_segments"]]
        except KeyError as e:
            print(e)

    definitions["files"] = set(item for item, _, _ in definitions["file_loc_host"])
    definitions["locations"] = set(item for _, item, _ in definitions["file_loc_host"])
    definitions["hostnames"] = set(item for _, _, item in definitions["file_loc_host"])

    for section, paradigm in [("POSIX", "posix"), ("MPIIO", "mpi")]:
        try:
            for batch in report.records[section]:
                counters["data"][(name_records[batch["id"]], batch["rank"])] = batch["counters"]
                counters["data"][(name_records[batch["id"]], batch["rank"])].update(batch["fcounters"])
                counters["keys"] |= batch["counters"].keys()
                counters["keys"] |= batch["fcounters"].keys()

        except KeyError as e:
            print(e)

    return events, definitions, counters
