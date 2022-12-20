
printf "Testing Java implementations...\n"
cd java/build
java -server -ea -Xms8G -Xmx8G -Xbootclasspath/a:'../lib/scala-library.jar:../lib/deuceAgent.jar' -jar experiments_instr.jar test
cd ../..
printf "Finished testing Java implementations\n"
