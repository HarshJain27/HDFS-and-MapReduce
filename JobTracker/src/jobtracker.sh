#! /bin/bash
`javac  -cp map_reduce.jar:. *.java -d ../bin`
`java  -cp map_reduce.jar:. job_tracker $1`
exit 0



