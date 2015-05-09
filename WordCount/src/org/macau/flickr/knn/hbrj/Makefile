HADOOP_VERSION=0.20.2
HADOOP_HOME=/usr/local/hadoop-${HADOOP_VERSION}
HADOOP_HOME=/home/hadoop/cz/hadoop-${HADOOP_VERSION}
JLIBS=${HADOOP_HOME}/lib/commons-lang-2.5.jar
LIBDIR=lib

MLIBS=${libjars}
MLIBS=elki.jar
JPATH=/usr/java/jdk1.7.0_03/bin#OPENJDK might not be able to compile the elki.jar, use sun JDK instead in this case
JC=${JPATH}/javac
#JC=javac

all:
	rm -rf knn
	mkdir knn
	${JC} -classpath ${HADOOP_HOME}/hadoop-${HADOOP_VERSION}-core.jar:${JLIBS}:${MLIBS} -g  -d knn *.java
	jar -cvf ./knn.jar -C knn/ .     # for hadoop
	#cp knn/test/* -r test

bulkload:  
	${JC} -g  de/lmu/ifi/dbs/elki/index/tree/spatial/rstarvariants/NonFlatRStarTree.java
	${JC} -g  de/lmu/ifi/dbs/elki/index/Zorder.java
	${JC} -g  de/lmu/ifi/dbs/elki/index/ExternalSort.java
	jar -uvf elki.jar \
	de/lmu/ifi/dbs/elki/index/Zorder*.class \
	de/lmu/ifi/dbs/elki/index/ExternalSort*.class
	#de/lmu/ifi/dbs/elki/index/tree/spatial/rstarvariants/Non*.class

#build elki.jar 
otherjar=${LIBDIR}/org.w3c.dom.svg_1.1.0.v200806040011.jar:${LIBDIR}/batik-all-1.7.jar:${LIBDIR}/commons-math-1.2.jar:${LIBDIR}/fop.jar:${LIBDIR}/batik-all-1.7.jar

elki:
	${JC} -classpath ${otherjar}:./ -g de/lmu/ifi/dbs/elki/*.java
	${JC} -classpath ${otherjar}:./ -g de/lmu/ifi/dbs/elki/*/*.java
	${JC} -classpath ${otherjar}:./ -g de/lmu/ifi/dbs/elki/*/*/*.java
	${JC} -classpath ${otherjar}:./ -g de/lmu/ifi/dbs/elki/*/*/*/*.java
	${JC} -classpath ${otherjar}:./ -g de/lmu/ifi/dbs/elki/*/*/*/*/*.java
	${JC} -classpath ${otherjar}:./ -g de/lmu/ifi/dbs/elki/*/*/*/*/*/*.java
	${JC} -classpath ${otherjar}:./ -g de/lmu/ifi/dbs/elki/*/*/*/*/*/*/*.java
	${JPATH}/jar -cvf elki.jar \
	de/lmu/ifi/dbs/elki/*.class  \
	de/lmu/ifi/dbs/elki/*/*.class \
	de/lmu/ifi/dbs/elki/*/*/*.class \
	de/lmu/ifi/dbs/elki/*/*/*/*.class \
	de/lmu/ifi/dbs/elki/*/*/*/*/*.class \
	de/lmu/ifi/dbs/elki/*/*/*/*/*/*.class \
	de/lmu/ifi/dbs/elki/*/*/*/*/*/*/*.class \
	de/lmu/ifi/dbs/elki/logging/logging-cli.properties
	
clean:
	rm -rf knn
	rm -rf knn.jar
	rm -rf *.class
	#rm elki.jar
