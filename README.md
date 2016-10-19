# BD-Pilaile
	- Basic implementation of frequency occurence with PAIRS, STRIPES and HYBRID Approach.

# Installation of Hadoop with cloudera
	1.	Download virtual box or VMware and set up on the system.
	2.	Download image file from cloudera and run it. 

# Development Process

	#Pairs Approach: 
			MapperClass
			-	extend Mapper
			
			-	override setup
				i.		Create a HashMap(A)

			-	override map method
				i.		split the input value into individual strings
				ii.		get one value and loop it to other neighbor elements according to the given window condition.
				iii.	add each of the value to the pairs
				iv. 	add each pair and its value to the HahsMap(A)
			-	override cleanup
				i.		Write all pair and its values from the HashMap(A)

	
		ReducerClass
			-	override setup
				i.	create a hashmap(B) and a marginal(M)
			-	override reduce
				i.		sum all the values(M) for total for each key of pair
				ii.		Sum all values for same pair (C)
				ii. 	calculate frequency (C/M)
				iii.	finally write the pair and its frequency

		Partitioner
			-	Partition it according to number of reducer


	
	#Stripes Approach: 
			MapperClass
			-	extend Mapper
			-	override setup
				i.		Create a HashMap(A)

			-	override map method
				i.		split the input value into individual strings
				ii.		get one value and loop it to other neighbor elements according to the given window condition.
				iii.	add each of the value to the a new hashmap(B)
				iv. 	add the new hashmap(B) to the hashmap(A) 

			-	override reduce method
				i.		Write the values of hashmap(A)
	
		ReducerClass
			-	override setup
				i.		create a hashmap(A), currentvalue(V) and a marginal(M) variables
			
			-	override reduce
				i.		sum all the values till a new currentValue(X) appears i.e marginal(M)
				ii.		calculate frequency per element on stripe
				iii.	finally write the new stripe from hashMap(A)
			
			-	override cleanup
				i.		Write the last value in the hashMap(A)

		Partitioner
			-	Partition it according to number of reducer

	#Hybrid Approach: 
			MapperClass
			-	extend Mapper
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
				i.		sum all the values and calculate total count (marginal) for each entry in reducer and save it in a stripe
				ii.		calculate frequency per element on the stripe
				iii.	finally write value and its stripe
		
		Partitioner
			-	Partition it according to number of reducer

# How to Run

# Local Mode -> Load it on IDE of your choice. Launch from specific package
# Pseudo Distributive mode
	- There are four shell scripts in this, initialize.sh, Hybrid.sh, Pairs.sh and Stripes.sh
	- Run initialize.sh to copy your file into Cloudera
	- Change permission of the shell script to 700 or as your wish. sudo chmod 655 *
	- Then run any of the other shell to get the output in your desktop. A new folder is created for each methods.
	

# Detail documentation is present with the doc 