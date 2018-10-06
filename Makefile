watch: InvertedIndex.jar Join.jar

clean:
	@rm InvertedIndex.jar
	@rm InvertedIndex/InvertedIndex*.class
	@rm ii.output
	@rm Join.jar
	@rm Join/Join*.class
	@rm j.output

test:
	@echo "Enter \"testii\" or \"testj\"."

testii: InvertedIndex.jar
	@echo -e "\033[1;37mInitializing HDFS structure...\033[0;37m"
	@hdfs dfs -rm -r -f /user/input /user/output
	@hdfs dfs -mkdir -p /user /user/input
	@hdfs dfs -put -f InvertedIndex/input/* /user/input
	@echo -e "\033[1;37m\nRunning InvertedIndex Program...\033[0;37m"
	@hadoop jar InvertedIndex.jar InvertedIndex /user/input /user/output
	@echo -e "\033[1;37m\nOutput will saved to <ii.output>\033[0;37m"
	@hdfs dfs -get -f /user/output/part-r-00000 ii.output
	@echo -e "\033[1;37m\nDifferences\033[0;37m"
	@-diff InvertedIndex/output/part-r-00000 ii.output
	@echo -e "\033[0m"	

testj: Join.jar
	@echo -e "\033[1;37mInitializing HDFS structure...\033[0;37m"
	@hdfs dfs -rm -r -f /user/input /user/output
	@hdfs dfs -mkdir -p /user /user/input
	@hdfs dfs -put -f Join/data/* /user/input
	@echo -e "\033[1;37m\nRunning Join Program...\033[0;37m"
	@hadoop jar Join.jar Join /user/input/records /user/output/join order,line_item 1 1
	@echo -e "\033[1;37m\nOutput will saved to <j.output>\033[0;37m"
	@hdfs dfs -get -f /user/output/join/part-r-00000 j.output
	@echo -e "\033[1;37m\nDifferences\033[0;37m"
	@-diff Join/output/part-r-00000 j.output
	@echo -e "\033[0m"
	
InvertedIndex.jar: InvertedIndex/InvertedIndex.java
	@echo -e "\033[1;37mBuild: InvertedIndex.jar\033[0;37m"
	@cd InvertedIndex; hadoop com.sun.tools.javac.Main InvertedIndex.java
	@cd InvertedIndex; jar cf InvertedIndex.jar InvertedIndex*.class
	@mv InvertedIndex/InvertedIndex.jar .
	@echo -e "\033[0m"
	
Join.jar: Join/Join.java
	@echo -e "\033[1;37mBuild: Join.jar\033[0;37m"
	@cd Join; hadoop com.sun.tools.javac.Main Join.java
	@cd Join; jar cf Join.jar Join*.class
	@mv Join/Join.jar .
	@echo -e "\033[0m"

