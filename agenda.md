[x] enable multiple datafiles support -> after a threshold size of file reaches, create new datafile and continue writing to it.
[x] merging process -> merge inactive datafiles into a multiple inactive datafiles
                    -> 12MB total, no of files should be 12MB/thresold
[ ] confirm partial writes -> if 2MB is of active segments, then new file should be written for 2MB and continued for next
    .idfile write
