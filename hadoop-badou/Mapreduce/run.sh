HADOOP_CMD="/usr/local/src/hadoop-1.2.1/bin/hadoop"
STREAM_JAR_PATH="/usr/local/src/hadoop-1.2.1/contrib/streaming/hadoop-streaming-1.2.1.jar"

INPUT_FILE_PATH="/input.data"
OUTPUT_FILE_PATH="/output.data"

$HADOOP_CMD fs -rmr -skipTrash $OUTPUT_FILE_PATH

$HADOOP_CMD jar $STREAM_JAR_PATH -input $INPUT_FILE_PATH -output $OUTPUT_FILE_PATH -mapper "python map.py" -reducer "python red.py" -file ./map.py -file ./red.py