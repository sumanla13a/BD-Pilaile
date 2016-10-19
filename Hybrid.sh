rm -r ~/Desktop/HybridMethodOp
hadoop fs -rm -r /user/cloudera/hybridAlgorithmOutput/
mkdir ~/Desktop/HybridMethodOp
hadoop jar /home/cloudera/Desktop/Full.jar com.suman.BD.HybridMethod.HybridAlgorithmImplementation /user/cloudera/pairsAlgorithmInput/
hadoop fs -get /user/cloudera/hybridAlgorithmOutput/* /home/cloudera/Desktop/HybridMethodOp
