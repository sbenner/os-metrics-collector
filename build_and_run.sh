
path=`pwd`
mvn clean
mvn install -pl commons -am
mvn package -T 1C -Dmaven.test.skip=true -Dcheckstyle.skip
mvn test
cd producer/target/
nohup java -Xmx2g -jar producer-1.0-SNAPSHOT.jar &
cd $path
cd consumer/target
nohup java -Xmx2g -jar consumer-1.0-SNAPSHOT.jar &
