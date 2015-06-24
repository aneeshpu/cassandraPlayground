# cassandraPlayground

#build
/.gradlew clean shadowJar

#create a notification
java -jar console/build/libs/console-2.4-all.jar -c foo

#read a notification
java -jar console/build/libs/console-2.4-all.jar -r foo

#update a notification
java -jar console/build/libs/console-2.4-all.jar -u foo

#delete a notification
java -jar console/build/libs/console-2.4-all.jar -d foo
