rm -r ~/Desktop/HybridMethodOp
hadoop fs -rm -r /user/cloudera/hybridAlgorithmOutput/
mkdir ~/Desktop/HybridMethodOp
hadoop jar $1 com.suman.BD.HybridMethod.HybridAlgorithmImplementation /user/cloudera/pairsAlgorithmInput/
hadoop fs -get /user/cloudera/hybridAlgorithmOutput/* /home/cloudera/Desktop/HybridMethodOp
