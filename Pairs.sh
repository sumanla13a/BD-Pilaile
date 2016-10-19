
rm ~/Desktop/PairsMethodOp -r
hadoop fs -rm -r /user/cloudera/pairsAlgorithmOutput/
mkdir ~/Desktop/PairsMethodOp
hadoop jar $1 com.suman.BD.PairsMethod.PairsAlgorithmImplementation /user/cloudera/pairsAlgorithmInput/
hadoop fs -get /user/cloudera/pairsAlgorithmOutput/* /home/cloudera/Desktop/PairsMethodOp
