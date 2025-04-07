Steps
1. Install Java with JDK 8 or higher
2. Set Java as a system variable
3. Download Apache Kafka (kafka_2.13-3.5.1.tgz)
4. Extract the zip file
5. Put the extracted files to C:\kafka
6. Change the value of dataDir in zooKeeper.properties file
7. Create a folder zookeeper-data
8. Change the value of log.dirs in server.properties file
9. Create a folder kafka-logs
10. Start ZooKeeper server, go to C:\kafka and enter a command .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
In this step (step 10), I got an error "The system cannot find the path specified."
I reinstalled Java and restarted my laptop. I still got the same error. I tried to run as an admin and it worked.
11. Start Kafka server, go to C:\kafka and enter a command .\bin\windows\kafka-server-start.bat .\config\server.properties 