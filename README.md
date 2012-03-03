# Pangool Bootstrap

This project contains an example project that can be used to start
to develop [Pangool](http://pangool.net) applications

## Compiling
'''
mvn install
'''

## Testing
'''
mvn test
'''

## Executing sort example
'''
cd app-module
mvn exec:java -Dexec.mainClass="com.datasalt.pangool.bootstrap.Driver" -Dexec.args="sort pom.xml pom.xml.copy"
'''

## Excuting with Hadoop
mvn install
cd app-module/target
hadoop jar pangool-bootstrap-0.40-job.jar sort ../pom.xml pomout

(This is currently not working in local mode because of Jackson version incompatibility
between Pangool and Apache Hadoop)


