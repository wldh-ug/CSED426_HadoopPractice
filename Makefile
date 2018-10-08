all: InvertedIndex.jar Join.jar MatrixMultiplication1_1.jar MatrixMultiplication1_2.jar MatrixMultiplication2.jar

help:
	@echo -e "\033[1;37mAvailable commands\n\033[0;37m"
	@echo -e "\tall       Build all jars"
	@echo -e "\tii        Build InvertedIndex.jar"
	@echo -e "\tj         Build Join.jar"
	@echo -e "\tm11       Build MatrixMultiplication1_1.jar"
	@echo -e "\tm12       Build MatrixMultiplication1_2.jar"
	@echo -e "\tm2        Build MatrixMultiplication2.jar\n"
	@echo -e "\tclean     Clear all created files"
	@echo -e "\tcleanii   Clear created files of InvertedIndex"
	@echo -e "\tcleanj    Clear created files of Join"
	@echo -e "\tcleanm11  Clear created files of MatrixMultiplication1_1"
	@echo -e "\tcleanm12  Clear created files of MatrixMultiplication1_2"
	@echo -e "\tcleanm2   Clear created files of MatrixMultiplication2\n"
	@echo -e "\ttest      Test all items"
	@echo -e "\ttestii    Test InvertedIndex"
	@echo -e "\ttestj     Test Join"
	@echo -e "\ttestm11   Test MatrixMultiplication1_1"
	@echo -e "\ttestm12   Test MatrixMultiplication1_2"
	@echo -e "\ttestm2    Test MatrixMultiplication2\n"
	@echo -e "\thelp      This help message"
	@echo -e "\033[0m"

ii: InvertedIndex.jar

j: Join.jar

m11: MatrixMultiplication1_1.jar

m12: MatrixMultiplication1_2.jar

m2: MatrixMultiplication2.jar

clean:
	@-rm InvertedIndex.jar InvertedIndex/InvertedIndex*.class ii.output
	@-rm Join.jar Join/Join*.class j.output
	@-rm MatrixMultiplication1_1.jar MatrixMultiplication/Multiplication1_1*.class m11.output
	@-rm MatrixMultiplication1_2.jar MatrixMultiplication/Multiplication1_2*.class m12.output
	@-rm MatrixMultiplication2.jar MatrixMultiplication/Multiplication2*.class m2.output

cleanii:
	@-rm InvertedIndex.jar InvertedIndex/InvertedIndex*.class ii.output

cleanj:
	@-rm Join.jar Join/Join*.class j.output

cleanm11:
	@-rm MatrixMultiplication1_1.jar MatrixMultiplication/Multiplication1_1*.class m11.output

cleanm12:
	@-rm MatrixMultiplication1_2.jar MatrixMultiplication/Multiplication1_2*.class m12.output

cleanm2:
	@-rm MatrixMultiplication2.jar MatrixMultiplication/Multiplication2*.class m2.output

test: testii testj testm11 testm12 testm2

testii: InvertedIndex.jar
	@echo -e "\033[1;37mInitializing HDFS structure...\033[0;37m"
	@hdfs dfs -rm -r -f /user/input /user/output
	@hdfs dfs -mkdir -p /user /user/input
	@hdfs dfs -put -f InvertedIndex/input/* /user/input
	@echo -e "\033[1;37m\nRunning InvertedIndex Program...\033[0;37m"
	@hadoop jar InvertedIndex.jar InvertedIndex /user/input /user/output
	@echo -e "\033[1;37m\nOutput will be saved to <ii.output>\033[0;37m"
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
	@echo -e "\033[1;37m\nOutput will be saved to <j.output>\033[0;37m"
	@hdfs dfs -get -f /user/output/join/part-r-00000 j.output
	@echo -e "\033[1;37m\nDifferences\033[0;37m"
	@-diff Join/output/part-r-00000 j.output
	@echo -e "\033[0m"
	
testm11: MatrixMultiplication1_1.jar
	@echo -e "\033[1;37mInitializing HDFS structure...\033[0;37m"
	@hdfs dfs -rm -r -f /user/input /user/output
	@hdfs dfs -mkdir -p /user /user/input
	@hdfs dfs -put -f MatrixMultiplication/data/* /user/input
	@echo -e "\033[1;37m\nRunning Multiplication1_1 Program...\033[0;37m"
	@hadoop jar MatrixMultiplication1_1.jar Multiplication1_1 /user/input/matrix1_1 /user/output/multiple1_1 3 5 2
	@echo -e "\033[1;37m\nOutput will be saved to <m11.output>\033[0;37m"
	@hdfs dfs -get -f /user/output/multiple1_1/part-r-00000 m11.output
	@echo -e "\033[1;37m\nDifferences\033[0;37m"
	@-diff MatrixMultiplication/output/multiple1_1/part-r-00000 m11.output
	@echo -e "\033[0m"

testm12: MatrixMultiplication1_2.jar
	@echo -e "\033[1;37mInitializing HDFS structure...\033[0;37m"
	@hdfs dfs -rm -r -f /user/input /user/output
	@hdfs dfs -mkdir -p /user /user/input
	@hdfs dfs -put -f MatrixMultiplication/data/* /user/input
	@echo -e "\033[1;37m\nRunning Multiplication1_2 Program...\033[0;37m"
	@hadoop jar MatrixMultiplication1_2.jar Multiplication1_2 /user/input/matrix1_1 /user/input/matrix1_2 /user/output/multiple1_2 3 5 2 3
	@echo -e "\033[1;37m\nOutput from phase 1 will be saved to <m11.output>\033[0;37m"
	@hdfs dfs -get -f /user/output/multiple1_2/part-r-00000 m11.output
	@echo -e "\033[1;37m\nOutput from phase 2 will be saved to <m12.output>\033[0;37m"
	@hdfs dfs -get -f /user/output/multiple1_2/final/part-r-00000 m12.output
	@echo -e "\033[1;37m\nDifferences from phase 1\033[0;37m"
	@-diff MatrixMultiplication/output/multiple1_2/part-r-00000 m11.output
	@echo -e "\033[1;37m\nDifferences from phase 2\033[0;37m"
	@-diff MatrixMultiplication/output/multiple1_2/final/part-r-00000 m12.output
	@echo -e "\033[0m"
	
testm2: MatrixMultiplication2.jar
	@echo -e "\033[1;37mInitializing HDFS structure...\033[0;37m"
	@hdfs dfs -rm -r -f /user/input /user/output
	@hdfs dfs -mkdir -p /user /user/input
	@hdfs dfs -put -f MatrixMultiplication/data/* /user/input
	@echo -e "\033[1;37m\nRunning Multiplication2 Program...\033[0;37m"
	@hadoop jar MatrixMultiplication2.jar Multiplication2 /user/input/matrix2 /user/output/multiple2 3 5 2 3
	@echo -e "\033[1;37m\nOutput will be saved to <m2.output>\033[0;37m"
	@hdfs dfs -get -f /user/output/multiple2/part-r-00000 m2.output
	@echo -e "\033[1;37m\nDifferences\033[0;37m"
	@-diff MatrixMultiplication/output/multiple2/part-r-00000 m2.output
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

MatrixMultiplication1_1.jar: MatrixMultiplication/Multiplication1_1.java
	@echo -e "\033[1;37mBuild: MatrixMultiplication1_1.jar\033[0;37m"
	@cd MatrixMultiplication; hadoop com.sun.tools.javac.Main Multiplication1_1.java
	@cd MatrixMultiplication; jar cf MatrixMultiplication1_1.jar Multiplication1_1*.class
	@mv MatrixMultiplication/MatrixMultiplication1_1.jar .
	@echo -e "\033[0m"

MatrixMultiplication1_2.jar: MatrixMultiplication/Multiplication1_1.java MatrixMultiplication/Multiplication1_2.java
	@echo -e "\033[1;37mBuild: MatrixMultiplication1_2.jar\033[0;37m"
	@cd MatrixMultiplication; hadoop com.sun.tools.javac.Main Multiplication1_1.java Multiplication1_2.java
	@cd MatrixMultiplication; jar cf MatrixMultiplication1_2.jar Multiplication1_*.class
	@mv MatrixMultiplication/MatrixMultiplication1_2.jar .
	@echo -e "\033[0m"

MatrixMultiplication2.jar: MatrixMultiplication/Multiplication2.java
	@echo -e "\033[1;37mBuild: MatrixMultiplication2.jar\033[0;37m"
	@cd MatrixMultiplication; hadoop com.sun.tools.javac.Main Multiplication2.java
	@cd MatrixMultiplication; jar cf MatrixMultiplication2.jar Multiplication2*.class
	@mv MatrixMultiplication/MatrixMultiplication2.jar .
	@echo -e "\033[0m"
