#! /bin/bash
`javac  -cp map_reduce.jar:. *.java -d ../bin`
`java  -cp map_reduce.jar:. TaskTracker `
exit 0



