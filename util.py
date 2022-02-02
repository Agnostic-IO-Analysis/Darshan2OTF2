import darshan
from event import Event

# keys = ["id", "rank", "hostname", "write_count", "read_count", "write_segments", "read_segments"]
# keys_r = ["offset", "length", "start_time", "end_time"]


def gather_stats_from_darshan(fp):
    stats = {"ranks": {}, "hostnames": set(), "paradigms": []}

    report = darshan.DarshanReport(fp, read_all=True, dtype="dict")
    job_data = report.data["metadata"]["job"]
    job_stats = {"start": job_data["start_time"],
                 "end": job_data["end_time"],
                 "nprocs": job_data["nprocs"],
                 "root_node": job_data["metadata"]["h"]}

    stats["job"] = job_stats

    stats["files"] = {file_id: {"name": file_name, "hostname": None} for file_id, file_name in report.name_records.items()}

    sec_par = []

    counter_keys = set()

    if "DXT_POSIX" in report.records.keys():
        sec_par.append(("DXT_POSIX", "posix"))
        stats["paradigms"].append("posix")
    if "DXT_MPIIO" in report.records.keys():
        sec_par.append(("DXT_MPIIO", "mpi"))
        stats["paradigms"].append("mpi")

    for section, paradigm in sec_par:

        for batch, cbatch in zip(report.records[section], report.records[section.removeprefix("DXT_")]):
            rank = {"hostname": batch["hostname"], "file_id": batch["id"],
                    "read_events": [Event("read", *elements.values(), paradigm, batch["id"]) for elements in
                                    batch["read_segments"]],
                    "write_events": [Event("write", *elements.values(), paradigm, batch["id"]) for elements in
                                     batch["write_segments"]],
                    "count_dict": cbatch["counters"]}

            stats["ranks"][batch["rank"]] = rank

            stats["hostnames"].add(batch["hostname"])

            stats["files"][batch["id"]]["hostname"] = batch["hostname"]

            # might be moved one scope up ?
            counter_keys = counter_keys.union(cbatch["counters"].keys())

    if "STDIO" in report.records.keys():
        for batch in report.records["STDIO"]:
            rank = {
                "hostname": "undefined",
                "file_id": batch["id"],
                "read_events": [],
                "write_events": [],
                "count_dict": batch["counters"]}

            stats["ranks"][batch["rank"]] = rank
            stats["hostnames"].add("undefined")
            stats["files"][batch["id"]]["hostname"] = "undefined"

            # might be moved one scope up ?
            counter_keys = counter_keys.union(batch["counters"].keys())

    stats["counter_keys"] = counter_keys

    return stats


# gather_stats_from_darshan("./ior_easy_read.darshan")