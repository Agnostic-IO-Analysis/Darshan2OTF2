#! /usr/bin/env python3

import otf2
import argparse
import subprocess
import os
import copy

import util
from event import Event

def write_otf2_trace(fp_in, fp_output, timer_res):

    with otf2.writer.open(fp_output, timer_resolution=timer_res) as trace:

        events, definitions, counters = util.get_stats_from_darshan(fp_in)
        root_node = trace.definitions.system_tree_node("root_node")
        system_tree_nodes = {name: trace.definitions.system_tree_node(name, parent=root_node) for name in definitions["hostnames"]}
        paradigms = {}

        if "posix" in definitions["paradigms"]:
            paradigms["posix"] = trace.definitions.io_paradigm(identification="POSIX",
                                                               name="POSIX I/O",
                                                               io_paradigm_class=otf2.IoParadigmClass.SERIAL,
                                                               io_paradigm_flags=otf2.IoParadigmFlag.NONE)

        if "mpi" in definitions["paradigms"]:
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

        io_files = {}

        skip = ["<STDIN>", "<STDOUT>", "<STDERR>"]
        for file_name, _, hostname in definitions["file_loc_host"]:
            if file_name not in skip:
                io_files[file_name] = trace.definitions.io_regular_file(file_name, scope=system_tree_nodes.get(hostname))
                skip.append(file_name)

        io_handles = {}
        for file_name in definitions["files"]:
            if file_name in ["<STDIN>", "<STDOUT>", "<STDERR>"]:
                handle_name = file_name
                handle_flag = otf2.definitions.enums.IoHandleFlag.PRE_CREATED
            else:
                handle_name = ""
                handle_flag = otf2.IoHandleFlag.NONE

            for paradigm in definitions["paradigms"]:
                io_handles[(paradigm, file_name)] = trace.definitions.io_handle(file=io_files.get(file_name),
                                                                                name=handle_name,
                                                                                io_paradigm=paradigms.get(paradigm),
                                                                                io_handle_flags=handle_flag)

        skip = []
        location_groups = {}
        locations = {}
        for _, location, hostname in definitions["file_loc_host"]:
            if location not in skip:
                lg = location_groups[f"rank {location}"] = trace.definitions.location_group(f"rank {location}", system_tree_parent=system_tree_nodes.get(hostname))
                locations[f"rank {location}"] = trace.definitions.location("Master Thread", group=lg)
            skip.append(location)

        events.sort(key=lambda x: x.start_time)
        t_last = 0
        if len(events) > 0:
            t_start = events[0].get_start_time_ticks(timer_res)

        
        modifiedEvents = []
        trackerEndTime = {}
        
        for event in events:            
            if event.location in trackerEndTime: 
                tracked_end_time = trackerEndTime[event.location][0]
                tracked_key = trackerEndTime[event.location][1]
                
                if tracked_end_time > event.start_time:
                    modifiedEvents.append(Event(event.action, 
                        event.offset, 
                        event.size, 
                        event.start_time, 
                        event.end_time, 
                        event.paradigm, 
                        event.file_name, 
                        event.location, 
                        event.hostname))
                    
                    modifiedEvents.append(Event(modifiedEvents[tracked_key].action, 
                        modifiedEvents[tracked_key].offset, 
                        modifiedEvents[tracked_key].size, 
                        event.end_time, 
                        modifiedEvents[tracked_key].end_time, 
                        modifiedEvents[tracked_key].paradigm,
                        modifiedEvents[tracked_key].file_name, 
                        modifiedEvents[tracked_key].location, 
                        modifiedEvents[tracked_key].hostname))
                    trackerEndTime[event.location] = [modifiedEvents[tracked_key].end_time, len(modifiedEvents) - 1]
                    modifiedEvents[tracked_key].end_time = event.start_time
                    
                    test = modifiedEvents[-1]
                    start_time = test.get_start_time_ticks(timer_res)-t_start
                    end_time = test.get_end_time_ticks(timer_res)-t_start
                    #print(trackerEndTime)
                    #print("SPLITTED : ", test.paradigm, " - ", test.location, " - ", start_time, " - ", end_time)
                else: 
                    modifiedEvents.append(Event(event.action, 
                        event.offset, 
                        event.size, 
                        event.start_time, 
                        event.end_time, 
                        event.paradigm, 
                        event.file_name, 
                        event.location, 
                        event.hostname))
                    trackerEndTime[event.location] = [event.end_time, len(modifiedEvents) - 1]
            else:
                modifiedEvents.append(Event(event.action, 
                    event.offset, 
                    event.size, 
                    event.start_time, 
                    event.end_time, 
                    event.paradigm, 
                    event.file_name, 
                    event.location, 
                    event.hostname))
                trackerEndTime[event.location] = [event.end_time, len(modifiedEvents) - 1]
                
        #print("printing out MODIFIED EVENTS")
        #counter = 0    
        #for event in modifiedEvents:
        #    start_time = event.get_start_time_ticks(timer_res)-t_start
        #    end_time = event.get_end_time_ticks(timer_res)-t_start
        #    print(counter ," : ", event.paradigm, " - ", event.location, " - ", start_time, " - ", end_time)
        #    counter = counter+1
        
        
        print("printing out event POSIX")
        for event in modifiedEvents:
            writer = trace.event_writer_from_location(locations.get(f"rank {event.location}"))
            io_mode = otf2.IoOperationMode.WRITE if event.action == "write" else otf2.IoOperationMode.READ
            
            start_time = event.get_start_time_ticks(timer_res)-t_start
            end_time = event.get_end_time_ticks(timer_res)-t_start
                
            if event.paradigm == "asdf":
                print(event.paradigm, " - ", event.location, " - ", start_time, " - ", end_time)
                writer.enter(start_time, regions.get((event.paradigm, event.action)))
                    
                writer.io_operation_begin(time=start_time,
                        handle=io_handles.get((event.paradigm, event.file_name)),
                        mode=io_mode,
                        operation_flags=otf2.IoOperationFlag.NONE,
                        bytes_request=event.size,
                        matching_id=0)
                        
                writer.io_operation_complete(time=end_time,
                        handle=io_handles.get((event.paradigm, event.file_name)),
                        bytes_result=event.size,
                        matching_id=0)
                    
                writer.leave(end_time, regions.get((event.paradigm, event.action)))
             
            #print("EVERYTHING")
            display = True    
            if display: 
                print(event.paradigm, " - ", event.location," - ", start_time, " - ", end_time)
                writer.enter(start_time, regions.get((event.paradigm, event.action)))
                    
                writer.io_operation_begin(time=start_time,
                        handle=io_handles.get((event.paradigm, event.file_name)),
                        mode=io_mode,
                        operation_flags=otf2.IoOperationFlag.NONE,
                        bytes_request=event.size,
                        matching_id=0)
                        
                writer.io_operation_complete(time=end_time,
                        handle=io_handles.get((event.paradigm, event.file_name)),
                        bytes_result=event.size,
                        matching_id=0)
                    
                writer.leave(end_time, regions.get((event.paradigm, event.action)))
                
            t_last = end_time
        
        # metrics
        metric_members = {}
        metric_classes = {}
        metric_instances = {}

        for (file_name, location), counters in counters["data"].items():

            lid = location
            location = locations.get(f"rank {location}")
            if location is None:
                print(f"location {lid} not found")
                continue

            for counter_key, counter_value in counters.items():
                if counter_value > 0:
                    if counter_key not in metric_members.keys():
                        metric_member = trace.definitions.metric_member(name=counter_key, metric_mode=otf2.MetricMode.ABSOLUTE_LAST)
                        
                        metric_members.update({counter_key: metric_member})
                        metric_class = trace.definitions.metric_class(members=(metric_members.get(counter_key),))
                        metric_classes.update({counter_key: metric_class})

                    if metric_instances.get((metric_classes.get(counter_key), location)) is None:
                        metric_instance = trace.definitions.metric_instance(metric_class=metric_classes.get(counter_key),recorder=location, scope=location)
                        metric_instances[(metric_classes.get(counter_key), location)] = metric_instance

                    metric_instance = metric_instances[(metric_classes.get(counter_key), location)]
                    writer.metric(time=t_last, metric=metric_instance, values=counter_value)

                    t_last += 1


def main():

    ap = argparse.ArgumentParser()
    ap.add_argument("file", type=str, help="file path to the darshan trace file")
    ap.add_argument("-o", "--output", type=str, help="specifies different output path, default is ./trace_out")
    ap.add_argument("-t", "--timer", type=int, help="sets timer resolution, default is 1e9")
    args = ap.parse_args()

    fp_in = args.file
    fp_out = "./trace_out" if args.output is None else args.output
    timer_res = int(1e9) if args.timer is None else args.timer

    if os.path.isdir(fp_out):
        subprocess.run(["rm", "-rf", fp_out])

    write_otf2_trace(fp_in, fp_out, timer_res)


if __name__ == '__main__':
    main()
