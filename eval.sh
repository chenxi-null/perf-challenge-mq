#cd ./target &&

#sudo sysctl -w kern.maxfilesperproc=$fn
#sudo launchctl limit maxfiles $fn unlimited

java -Xms6144m -Xmx6144m -XX:MaxDirectMemorySize=2048m -jar ./target/mq-sample.jar

java -Xms6144m -Xmx6144m -XX:MaxDirectMemorySize=2048m -jar ./target/mq-sample.jar

java -Xms6144m -Xmx6144m -XX:MaxDirectMemorySize=2048m -jar ./target/mq-sample.jar
