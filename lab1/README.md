# Lab 1 - Hadoop WordCount

A simple MapReduce WordCount program running on a 3-node Hadoop cluster (1 master + 2 slaves) built with Docker.

Module: Big Data - ESI-SBA (2026-2027)

## What it does

The program reads `purchases.txt` from HDFS, splits every line into words, and counts how many times each word appears.

```
Input (HDFS)  ->  Map (word, 1)  ->  Shuffle (group by word)  ->  Reduce (sum)  ->  Output (HDFS)
```

## Project structure

```
wordcount/
├── pom.xml
└── src/main/java/tp1/
    ├── TokenizerMapper.java
    ├── IntSumReducer.java
    └── WordCount.java
```

| File | Role |
|---|---|
| `TokenizerMapper.java` | Map step: splits each line into words and emits `(word, 1)` |
| `IntSumReducer.java` | Reduce step: sums all the values of each word |
| `WordCount.java` | Driver: configures the job and submits it to the cluster |

## Requirements

- Docker
- JDK 17 (compiled for Java 8 with `maven.compiler.release=8`)
- Maven
- IntelliJ IDEA (optional)

## 1. Set up the cluster

```bash
docker pull liliasfaxi/spark-hadoop:hv-2.7.2

docker network create --driver=bridge hadoop

docker run -itd --net=hadoop -p 50070:50070 -p 8088:8088 -p 16010:16010 --name hadoop-master --hostname hadoop-master liliasfaxi/spark-hadoop:hv-2.7.2

docker run -itd -p 8040:8042 --net=hadoop --name hadoop-slave1 --hostname hadoop-slave1 liliasfaxi/spark-hadoop:hv-2.7.2

docker run -itd -p 8041:8042 --net=hadoop --name hadoop-slave2 --hostname hadoop-slave2 liliasfaxi/spark-hadoop:hv-2.7.2
```

Enter the master and start Hadoop:

```bash
docker exec -it hadoop-master bash
./start-hadoop.sh
```

## 2. Load the data into HDFS

```bash
hadoop fs -mkdir -p input
hadoop fs -put purchases.txt input
hadoop fs -ls input
```

## 3. Build the jar

On your own machine:

```bash
mvn clean package
docker cp target/wordcount-1.0-SNAPSHOT.jar hadoop-master:/root/wordcount.jar
```

## 4. Run the job

Inside the `hadoop-master` container:

```bash
hdfs dfs -ls /user/root/input

hadoop jar /root/wordcount.jar tp1.WordCount /user/root/input /user/root/output

hdfs dfs -ls /user/root/output

hdfs dfs -cat /user/root/output/part-r-00000
```

| Argument | Meaning |
|---|---|
| `/root/wordcount.jar` | The compiled Java application |
| `tp1.WordCount` | The driver class to run |
| `/user/root/input` | Input directory in HDFS |
| `/user/root/output` | Output directory in HDFS (must not exist yet) |

The result is in `part-r-00000`. Each line is a word followed by its count.

## Open the project again later

Containers keep their data when stopped, so do not run `docker run` again.

```bash
docker start hadoop-master hadoop-slave1 hadoop-slave2
docker exec -it hadoop-master bash
./start-hadoop.sh
hdfs dfs -cat /user/root/output/part-r-00000
```

To run the job again, delete the old output first:

```bash
hdfs dfs -rm -r /user/root/output
```

Web interfaces:

- NameNode: http://localhost:50070
- YARN: http://localhost:8088

## Common errors

| Error | Fix |
|---|---|
| `Output directory already exists` | `hdfs dfs -rm -r /user/root/output` |
| `UnsupportedClassVersionError` | Compile with `maven.compiler.release=8` |
| `ClassNotFoundException` | Use the full name `tp1.WordCount` |
| `Type mismatch in value from map` | Check `setOutputKeyClass` / `setOutputValueClass` in the driver |
