# BD-Pilaile
	- Basic implementation of frequency occurence with PAIRS, STRIPES and HYBRID Approach.

# Installation of Hadoop with cloudera
	1.	Download virtual box or VMware and set up on the system.
	2.	Download image file from cloudera and run it. 

# Development Process

	#Pairs Approach: 
			MapperClass
			-	extend Mapper
			-	uses custom class Pair, DoubleWritable, Text and IntWritable
			-	override setup
				i.		Create a HashMap(A)

			-	override map method
				i.		split the input value into individual strings
				ii.		get one value and loop it to other neighbor elements according to the given window condition.
				iii.	add each of the value to the pairs
				iv.		choose a special character that will not appear in the data to create an extra pair of values 1 for total count.
				v.	 	add/update each pair and its value to the HahsMap(A)

			-	override cleanup
				i.		Write all pair and its values from the HashMap(A)

	
		ReducerClass
			-	override setup
				i.	create a hashmap(B) and a marginal(M)

			-	override reduce
				i.		sum all the special pair for total(M)
				ii.		Sum all values for common pair (C)
				iii. 	calculate frequency (C/M) for each pair
				iv.		finally write the pair and its frequency

		Partitioner
			-	Partition it according to number of reducer


	
	#Stripes Approach: 
			MapperClass
			-	extend Mapper
			-	uses custom class CustomMapWritable, DoubleWritable, Text from hadoop library
			-	override setup
				i.		Create a HashMap(A)

			-	override map method
				i.		split the input value into individual strings
				ii.		get one value and loop it to other neighbor elements according to the given window condition.
				iii.	add each of the value to the a new hashmap(B)
				iv. 	add/update the new hashmap(B) to the hashmap(A) 

			-	override reduce method
				i.		Write the values of hashmap(A)
	
		ReducerClass
			-	override setup
				i.		create a hashmap(A) and a marginal(M) variables
			
			-	override reduce
				i.		sum all the common values occurence in the list of stripes of reducer input
				ii.		calculate total count (M) of all element in the reducer input
				iii.	calculate frequency per element on the stripe
				iv.		Update hashmap(A) for each value with frequency
				iv.		finally write value and its stripe containing the frequency


		Partitioner
			-	Partition it according to number of reducer

	#Hybrid Approach: 
			MapperClass
			-	extend Mapper
			-	uses custom class Pair, CustomMapWritable, DoubleWritable, Text
			-	override setup
				i.		Create a HashMap(A)

			-	override map method
				i.		split the input value into individual strings
				ii.		get one value and loop it to other neighbor elements according to the given window condition.
				iii.	add each of the value to a pair (P)
				iv. 	add the pair (P) to the hashmap(A) 

			-	override reduce method
				i.		Write the values of hashmap(A)
	
		ReducerClass
			-	override setup
				i.		create a hashmap, currentvalue and a marginal variables
				
			- override reduce
				i.		sum all the values of common pairs occuring in the list of reducer input
				ii.		calculate total count (marginal) till a new pair appears with different key
				iii.	add the count to hashmap
				iv.		repeat 1-3 and update hashmap as required
			
			-	override cleanup
				i.		read hashmap and calculate frequency with the marginal and each value (eachvalue/marginal)
				ii.		update the hashmap with frequency
				iii.	write output

		Partitioner
			-	Partition it according to number of reducer

# How to Run

# Local Mode -> 
	-	Load it on IDE of your choice.
	-	Import all the library jars from Hadoop, lib and client-2.0
	-	Launch from specific package
# Pseudo Distributive mode
	- There are four shell scripts in this, initialize.sh, Hybrid.sh, Pairs.sh and Stripes.sh
	- Run initialize.sh to copy your file into Cloudera
	- Change permission of the shell script to 700 or as your wish. sudo chmod 655 *
	- Then run any of the other shell to get the output in your desktop. A new folder is created for each methods.
	

# Detail documentation is present with the doc as javaDoc