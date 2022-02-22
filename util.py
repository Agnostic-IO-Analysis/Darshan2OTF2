import darshan
from event import Event


def gather_stats_from_darshan(fp):
    stats = {"paradigms": {}}

    report = darshan.DarshanReport(fp, read_all=True, dtype="dict")

    keys = []
    if "DXT_POSIX" in report.records.keys():
        keys.append(("DXT_POSIX", "POSIX", "posix"))
        stats["paradigms"]["posix"] = {"files": {}}
    if "DXT_MPIIO" in report.records.keys():
        keys.append(("DXT_MPIIO", "MPI-IO", "mpi"))
        stats["paradigms"]["mpi"] = {"files": {}}

    stats["paradigms"]["std"] = {"files": {}}
    for section, csection, paradigm in keys:
        for batch, cbatch in zip(report.records[section], report.records[csection]):
            stats["paradigms"][paradigm]["files"][(batch["id"], report.name_records[batch["id"]])] = {
                "read_events": [Event("read", *elements.values(), paradigm, batch["id"]) for elements in
                                batch["read_segments"]],
                "write_events": [Event("write", *elements.values(), paradigm, batch["id"]) for elements in
                                 batch["write_segments"]],
                "counters": cbatch["counters"],
                "hostname": batch["hostname"],
                "rank": batch["rank"],
            }

    for cbatch in report.records["STDIO"]:
        stats["paradigms"]["std"]["files"][(cbatch["id"], report.name_records[cbatch["id"]])] = {
            "read_events": [],
            "write_events": [],
            "counters": cbatch["counters"],
            "hostname": "undefined",
            "rank": cbatch["rank"],
        }

    job_data = report.data["metadata"]["job"]
    stats["job"] = {
        "start": job_data["start_time"],
        "end": job_data["end_time"],
        "nprocs": job_data["nprocs"],
        "root_node": job_data["metadata"]["h"]}

    return stats


def get_hostnames(stats):
    hostnames = set()
    for paradigm in stats["paradigms"]:
        for file_id, file_name in stats["paradigms"][paradigm]["files"]:
            hostnames.add(stats["paradigms"][paradigm]["files"][(file_id, file_name)]["hostname"])
    return hostnames


def get_counter_keys(stats):
    counter_keys = set()
    for paradigm in stats["paradigms"]:
        for file_id, file_name in stats["paradigms"][paradigm]["files"]:
            for k in stats["paradigms"][paradigm]["files"][(file_id, file_name)]["counters"].keys():
                counter_keys.add(k)
    return counter_keys


def get_files(stats):
    # (file_id, file_name, hostname)
    files = set()
    for paradigm in stats["paradigms"]:
        for file_id, file_name in stats["paradigms"][paradigm]["files"]:
            files.add((file_id, file_name, stats["paradigms"][paradigm]["files"][(file_id, file_name)]["hostname"]))
    return files


def get_ranks_hostnames(stats):
    ranks = set()
    for paradigm in stats["paradigms"]:
        for file_id, file_name in stats["paradigms"][paradigm]["files"]:
            ranks.add((stats["paradigms"][paradigm]["files"][(file_id, file_name)]["rank"], stats["paradigms"][paradigm]["files"][(file_id, file_name)]["hostname"]))
    return ranks
