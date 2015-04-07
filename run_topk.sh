#~ /bin/bash
mvn package
storm jar target/storm-starter-0.9.3-jar-with-dependencies.jar storm.starter.trident.project.countmin.TopKTopology
