watch: WordCount.jar InvertedIndex.jar

clean:
	@rm WordCount.jar
	@rm WordCount/WordCount*.class
	@rm wc.output
	@rm InvertedIndex.jar
	@rm InvertedIndex/InvertedIndex*.class
	@rm ii.output

test:
	@echo "Enter \"testii\" or \"testwc\"."

testii: InvertedIndex.jar
	@echo -e "\033[1;37mInitializing HDFS structure...\033[0;37m"
	@hdfs dfs -mkdir -p /user /user/input
	@hdfs dfs -rm -r -f /user/output
	@hdfs dfs -put -f InvertedIndex/input/* /user/input
	@echo -e "\033[1;37mRunning InvertedIndex Program...\033[0;37m"
	@hadoop jar InvertedIndex.jar InvertedIndex /user/input /user/output
	@echo -e "\033[1;37mOutput will saved to <ii.output>\033[0;37m"
	@hdfs dfs -get -f /user/output/part-r-00000 ii.output
	@echo -e "\033[1;37mDifferences\033[0;37m"
	@-diff InvertedIndex/output/part-r-00000 ii.output
	@echo -e "\033[0m"
	
testwc: testWordCount

WordCount.jar: WordCount/WordCount.java
	@echo -e "\033[1;37mBuild: WordCount.jar\033[0;37m"
	@cd WordCount; hadoop com.sun.tools.javac.Main WordCount.java
	@cd WordCount; jar cf WordCount.jar WordCount*.class
	@mv WordCount/WordCount.jar .
	@echo -e "\033[0m"

InvertedIndex.jar: InvertedIndex/InvertedIndex.java
	@echo -e "\033[1;37mBuild: InvertedIndex.jar\033[0;37m"
	@cd InvertedIndex; hadoop com.sun.tools.javac.Main InvertedIndex.java
	@cd InvertedIndex; jar cf InvertedIndex.jar InvertedIndex*.class
	@mv InvertedIndex/InvertedIndex.jar .
	@echo -e "\033[0m"

