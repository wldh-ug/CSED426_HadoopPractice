# Assignment#1 Hadoop

I used Hadoop 2.8.1, following [the guideline](Hadoop Installation Guideline.pdf) provided by TA.

## How to compile and test

After installing hadoop, and launching HDFS and YARN using `start-dfs.sh` and `start-yarn.sh` as written in guideline, you can follow below to run my code.  

```bash
$ make test
```

This will build all homework items and automatically run jar in your Hadoop system and compare the result with standard result (provided by TA).  

For more information, you can type `make help` for full command list.  
## What is `watch` file?
It's linux shell script, if you run it, it tries to run `make all` every 0.5 seconds. That is, it helps you **automatically building jar files**.
