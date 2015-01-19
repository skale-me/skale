#! /bin/sh

# Nb web interface port = 50070

JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/
# HADOOP_PREFIX=/home/cedric/work/dist/hadoop-1.2.1
HADOOP_PREFIX=$HOME/work/dist/hadoop-1.2.1

# Downlad and install hadoop
# wget http://apache.crihan.fr/dist/hadoop/common/hadoop-1.2.1/hadoop-1.2.1.tar.gz
tar xvzf hadoop-1.2.1.tar.gz
# rm hadoop-1.2.1.tar.gz

# write hadoop-env.sh
cat <<EOT > $HADOOP_PREFIX/conf/hadoop-env.sh
# Root of Java installation
export JAVA_HOME=$JAVA_HOME

# Root of hadoop installation
export HADOOP_PREFIX=$HADOOP_PREFIX
EOT

# write core-site.xml
cat <<EOT > $HADOOP_PREFIX/conf/core-site.xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
    <property>
        <name>fs.default.name</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
EOT

# write hdfs-site.xml
cat <<EOT > $HADOOP_PREFIX/conf/hdfs-site.xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
EOT

# Enable passphraseless ssh
# ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa
# cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys

# Empty hdfs root dir, format hdfs and start daemon
cd $HADOOP_PREFIX
bin/hadoop namenode -format
bin/start-dfs.sh

# Delete previous Create test dir in hdfs
bin/hadoop dfs -rmr /test
bin/hadoop dfs -mkdir /test

# Generate data.txt in /tmp with ugrid data generator
../../ugrid/benchmark/gen_data.js /tmp/data.txt 256

# Copy /tmp/data.txt in hdfs test directory 
bin/hadoop dfs -put /tmp/data.txt /test
