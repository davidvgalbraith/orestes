wget http://mirrors.koehn.com/apache/cassandra/2.2.4/apache-cassandra-2.2.4-bin.tar.gz
tar -xf apache-cassandra-2.2.4-bin.tar.gz
rm apache-cassandra-2.2.4-bin.tar.gz

curl -L -O https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.0.0/elasticsearch-2.0.0.tar.gz
tar -xvf elasticsearch-2.0.0.tar.gz
rm elasticsearch-2.0.0.tar.gz
mkdir elasticsearch-2.0.0/config/scripts
cp scripts/aggkey.groovy elasticsearch-2.0.0/config/scripts/aggkey.groovy
