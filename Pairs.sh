#Assuming the jar is in Desktop

rm ~/Desktop/PairsMethodOp -r
hadoop fs -rm -r /user/cloudera/pairsAlgorithmOutput/
mkdir ~/Desktop/PairsMethodOp
hadoop jar /home/cloudera/Desktop/Full.jar com.suman.BD.PairsMethod.PairsAlgorithmImplementation /user/cloudera/pairsAlgorithmInput/
hadoop fs -get /user/cloudera/pairsAlgorithmOutput/* /home/cloudera/Desktop/PairsMethodOp
