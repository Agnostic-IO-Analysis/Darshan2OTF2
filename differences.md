# Differences



| subject | native otf2 | converted from darshan |
| ------- | ---- | ------- |
|filepath | /beegfs/.global0/old/ws/soeste-ior/datafiles/2021.12.02-22.10.19/ior-easy| /beegfs/global0/ws/soeste-ior/datafiles/2021.12.02-22.10.19/ior-easy |
| num io ops | 7.564.309 | 3.629.728 | 
| num io ops per process | 227.308 | 113.429 - 111.268 |
| size | 6.937 TiB | 6.923 TiB |
| time | 9.036,087 s | 9.492,34 s |
| invocations | 3.822.564 | 3.629.728 |
| trace size | 704 Mb | 145 Mb |
| otf2-profiler | wont run ? | ??? |
| num events | 1554332, 1554358, 1554100, 1554238, 1554192, 1554048, 1553046, 1554248, 1553638, 1553768, 1553730, 1553772, 1553896, 1551238, 1553706, 1553594, 1553310, 1553298, 1551348, 1553312, 1553300, 1553060, 1553274, 1553376, 1553328, 1553547, 1553403, 1554112, 1553400, 1553218, 1551484, 1553310 | 113429 * 32 (per rank) |
| num total events | 49.709.984 | 14.518.912 |
| num io events | 8.156.740 | 7.259.456 |
| io bandwidth | all from 828 Mib/s to 780 Mib/s | 16 * ~ 812 Mib/s, 16 * ~ 722 Mib7s |
| attributes | 4 | 0 |
| calling context prop | 0 | 0 |
| calling contexts | 0 | 0 |
| callpath param | 0 | 0 |
| callsites | 0 | 0 |
| cart cords | 32 | 0 |
| cart dim | 2 | 0 |
| cart topologies | 1 | 0 |
| clock prop global offset | 14215614748478244 | 214548064948 |
| clock prop timer res | 2494220032 | 1000000000 |
| clock prop trace len | 911364021177 | 371626144409 |
| comms | 12 | 0 |
| groups | 9 | 0 |
| interrupt gens | 0 | 0 |
| io directories | - | - |
| io file props | 23715 | 0 |
| io files | 7908 | 33 |
| io handles | 64083 | 36 | 
| io paradigms | 3 | 1 |
| io pre created handle states | 17 | 0 |
| io regular files | - | - |
| location group props | 0 | 0 |
| location groups | 32 | 32 |
| locations props | 0 | 0 |
| locations | 32 | 32 |
| metric class recorders | 0 | 0 |
| metric classes | - | - |
| metric instances | - | - |
| metric members | 0 | 0 |
| metrics | 0 | 0 |
| paradigm props | 2 | 0 |
| paradigms | 1 | 0 |
| parameters | 0 | 0 |
| regions | 460 | 4 |
| rma wins | 0 | 0 |
| source code locations | 0 | 0 | 
| strings | 2673 | 82 |
| system tree node domains | 5 | 0 |
| system tree node props | 1 | 0 |
| system tree nodes | 5 | 5 |
| io op begin per relevant file | 113653 | 113429 |


## only relevant files
| subject | native otf2 | converted from darshan |
|----|----|----|
| time | 9.036,087 s | 9.492,34 s |
| num io ops |7.273.858 | 3.629.728 | 
| io ops per process | 32x 227.308, 1x 2 | 113.429 x 32 |
| files with io | 33 = 32 + 1x stonewall file | 32 |
| aggregated transaction size | 32x 221.979 GiB | 32x 221.541 GiB |
| aggregated transaction time (s) | from 291.34 to 274.297 | from 314.664 to 277.887 | 
| io transaction size | 1.023,991 KiB | 2 MiB |
| max transaction size | 2 Mib, (0 Mibs stonewall file) | 2 Mib |
| min transaction size | 0 Mibs | 2 Mibs |
| avg transaction time | from 1,282 ms to 1,207, (513,02 us stonewall file) | 16x ~2,76 ms, 16x ~2,45 ms |
| min transaction time | ~ 1,3 us, (247,34 us stonewall file) | from 883,102 us to 755,787 |
| max transaction time | 3x ~ 20 s, 1x ~7,5 s 12x from 0,879 s to 0,181 s, (778,701 us stonewall file) | 2x 0,527 s, 2x 0,351 s, 12x from 0,179 s to 7,583 ms |
| avg io bandwidth | from 828,626 Mibs to 780,197 Mibs | 16x ~812 Mibs, 16x ~722 Mibs |
| max io bandwidth | from 2,728 Gibs to 2,405 Gibs | from 2,584 Gibs to 2,212 Gibs |
| min io bandwidth | 32x 0 | from 263,742 Mibs to 3,792 Mibs |
