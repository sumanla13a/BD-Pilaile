#mkdir /home/cloudera/Desktop/StripesMethodOp
if [ $(hadoop fs -test -d /user/cloudera/stripesAlgorithmOutput/) == 0 ]
then
	echo here
	$(hadoop fs -rm -rf /user/cloudera/stripesAlgorithmOutput/)
fi
#hadoop jar /home/cloudera/Desktop/Pairs.jar com.suman.BD.StripesMethod.StripesAlgorithmImplementation /user/cloudera/pairsAlgorithmInput/
#hadoop fs -get /user/cloudera/stripesAlgorithmOutput/part-r-00000 /home/cloudera/Desktop/StripesMethodOp
#hadoop fs -get /user/cloudera/stripesAlgorithmOutput/part-r-00001 /home/cloudera/Desktop/StripesMethodOp
