out_file=$1.jar
jar cf out_file $1*.class
hadoop jar out_file $1 /A4/$2 /A4/$3
