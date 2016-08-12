import scala.io.Source
import scala.math._
import java.io._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
import scala.collection.mutable.ListBuffer
//Imports for question B
// Import Row.
import org.apache.spark.sql.Row;
// Import Spark SQL data types
import org.apache.spark.sql.types.{StructType,StructField,StringType};

object SimpleApp {
	def main(args: Array[String]) {
		//The input file
		val dataFile = args(0)			

		val conf = new SparkConf().setAppName("Join Application")
		val sc = new SparkContext(conf)
		//Creating an RDD with the input file data
		val tableData = sc.textFile(dataFile, 10).cache()
		
		//Creating four RDDs one for each table (R,A,B,C)
		val tableR = tableData.filter(line => line.charAt(0) == 'R').persist
		val tableA = tableData.filter(line => line.charAt(0) == 'A').persist
		val tableB = tableData.filter(line => line.charAt(0) == 'B').persist
		val tableC = tableData.filter(line => line.charAt(0) == 'C').persist

                //!! The output on all questions has the format (Key Of A, Key Of B, Key Of C, townCode(R), Month(A), Item(B), Town(C))
		//For checking the results on each reducer

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		//Code for question A
		//Creating a Key-Value pair from table R and A for the Join function
		val timeStart = System.currentTimeMillis;
		val pairsR = tableR.map(x => (x.split(",")(1), x))
		val pairsA = tableA.map(x => (x.split(",")(1), x))		

		//Perfoming the Join function on the tables
		val joinR_A = pairsR.join(pairsA)

		//Getting the value of the pair converted to string so i can split it again 
		//in a key value pair for the second join
		var joinValues = joinR_A.values.map(x => x.toString)		
		
		//Creating a Key-Value pair from the joined R_A table and the B table 
		//for the second Join function
		val pairsR_A = joinValues.map(x => (x.split(",")(2), x))
		val pairsB = tableB.map(x => (x.split(",")(1), x))

		//Perfoming the Join function on the tables
		val joinR_A_B = pairsR_A.join(pairsB)
		
		//Getting the value of the pair converted to string so i can split it again 
		//in a key value pair for the third join
		joinValues = joinR_A_B.values.map(x => x.toString)

		//Creating a Key-Value pair from the joined R_A_B table and the C table 
		//for the third Join function
		val pairsR_A_B = joinValues.map(x => (x.split(",")(3), x))
		val pairsC = tableC.map(x => (x.split(",")(1), x))

		//Perfoming the Join function on the tables
		val joinR_A_B_C = pairsR_A_B.join(pairsC)
		val counter = joinR_A_B_C.count

		//Getting the values of the pair, for cleaning them up from table headers
		// and writing them on a file
		joinValues = joinR_A_B_C.values.map(x => x.toString)
		
		//removing the parentheses from the string
		val toRemove = "()".toSet
		val joinResult = joinValues.map(x => x.filterNot(toRemove))
		val cleanResults = joinResult.map(_.split(",")).map(x => (x(1) + "," + x(2) + "," + x(3) + "," + x(4) + "," + x(7) + "," + x(10) + "," + x(13)))
		cleanResults.saveAsTextFile(args(1) + "A-Results")			
		
		val timeA = System.currentTimeMillis - timeStart;
		println("Number of results of question A: " + counter)	
		println("Time passed in miliseconds: " + timeA + "\n")			

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		//Code for question B
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		sqlContext.setConf( "spark.sql.shuffle.partitions", "10")
		
		val schemaString_R = "A B C Number"
		val schemaString_A = "A Month"
		val schemaString_B = "B Type"
		val schemaString_C = "C Town"

		// Generate the schemas based on the string of schema
		val schema_R =  StructType(schemaString_R.split(" ").map(fieldName => 
			StructField(fieldName, StringType, true)))
		val schema_A =  StructType(schemaString_A.split(" ").map(fieldName => 
			StructField(fieldName, StringType, true)))
		val schema_B =  StructType(schemaString_B.split(" ").map(fieldName => 
			StructField(fieldName, StringType, true)))
		val schema_C =  StructType(schemaString_C.split(" ").map(fieldName => 
			StructField(fieldName, StringType, true)))

		// Convert records of the RDDs tableR, tableA, tableB, tableC  to Rows.
		val rowRDD_R = tableR.map(_.split(",")).map(p => Row(p(1), p(2), p(3), p(4).trim))
		val rowRDD_A = tableA.map(_.split(",")).map(p => Row(p(1), p(2).trim))
		val rowRDD_B = tableB.map(_.split(",")).map(p => Row(p(1), p(2).trim))
		val rowRDD_C = tableC.map(_.split(",")).map(p => Row(p(1), p(2).trim))

		// Apply the schema to the RDD.
		val dataFrame_R = sqlContext.createDataFrame(rowRDD_R, schema_R)
		val dataFrame_A = sqlContext.createDataFrame(rowRDD_A, schema_A)
		val dataFrame_B = sqlContext.createDataFrame(rowRDD_B, schema_B)
		val dataFrame_C = sqlContext.createDataFrame(rowRDD_C, schema_C)

		// Register the DataFrames as a table.
		dataFrame_R.registerTempTable("RTable")	
		dataFrame_A.registerTempTable("ATable")	   	
		dataFrame_B.registerTempTable("BTable")	   	
		dataFrame_C.registerTempTable("CTable")	   	   		 

		// SQL JOIN statements		
		val resultsFirstJoin = sqlContext.sql("SELECT RTable.*, ATable.Month FROM RTable INNER JOIN ATable ON RTable.A = ATable.A")
		resultsFirstJoin.registerTempTable("FirstJoinTable")
		val resultsSecondJoin = sqlContext.sql("SELECT FirstJoinTable.*, BTable.Type FROM FirstJoinTable INNER JOIN BTable ON FirstJoinTable.B = BTable.B") 
		resultsSecondJoin.registerTempTable("SecondJoinTable")
		val resultsThirdJoin = sqlContext.sql("SELECT SecondJoinTable.*, CTable.Town FROM SecondJoinTable INNER JOIN CTable ON SecondJoinTable.C = CTable.C")
		//counting the resukts that were generated
		val counterSQL = resultsThirdJoin.count
		//saving the results
	        resultsThirdJoin.map(x => x.toString).saveAsTextFile(args(1) + "B-Results")
		println("Number of results of question B: " + counterSQL)
		//calculating the proccessing time
		val timeB = System.currentTimeMillis -timeStart - timeA;
		println("Time passed in miliseconds: " + timeB + "\n")		

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		//Code for question C
		//Acording to the paper: Optimizing Multiway Joins in a Map-Reduce Environment
		//There is an optimal distribution of reducers to be assigned the key-value pairs of each relation on a star join scenario
		//Given 'k' the number of available reducers the schema of the i-th dimension table is "di * n-th root of(k/d)" 
		//Where d is the product of the number of values of all relations 
		
		//First we have to compute the number of tuples on each demension table (A,B,C)
		val numA = tableA.count
		val numB = tableB.count
		val numC = tableC.count
		//Given number of partitions
		val k = args(2).toInt
		//Computing the "share" of each relation (dimension tables)
		val shareA = round(cbrt((k * (numA * numA) / (numB * numC))))
		val shareB = round(cbrt((k * (numB * numB) / (numA * numC))))
		val shareC = round(cbrt((k * (numC * numC) / (numA * numB))))
		//The total reducers that we will use are the product of the shares
		val totalReducers = shareA.toInt * shareB.toInt * shareC.toInt		
		
		//To implement the method we have to create new keys for our relations pairs
		//First for the R relation
		// the keys for the R relation is generated by calculating the modulo of each key of relations A, B C with the calculated share of each one
		// and append them into one number
		// the relations A, B ,C will be merged into one table with key - value pairs the new key will be the modulo of each tables key appended with each other

		//calculating the hash function of each key of the R relation
		val relationR_hashedKeys = tableR.map(_.split(",")).map(x => ((x(1).toInt % shareA).toString + (x(2).toInt % shareB).toString + (x(3).toInt % shareC).toString + "," + x(1) + x(2) + x(3) + "," + x(4)))
		
		//spliting the the tuple into key value pairs
		val relationR_RDD = relationR_hashedKeys.map(x => (x.split(",")(0).toInt, x))
		
		//then for the other relations			
		val relationA = tableA.map(x => x.split(",")).collect
		val relationB = tableB.map(x => x.split(",")).collect
		val relationC = tableC.map(x => x.split(",")).collect

		// Buffer List with the merged values of all relations
		var mergedRelations = new ListBuffer[String]()

		//All relations will be merged into one with key the module of each relations key with the calculated share
		for(x <- relationA){
			for(y <- relationB){
				for(z <- relationC){					
					mergedRelations += (x(1).toInt % shareA).toString + (y(1).toInt % shareB).toString + (z(1).toInt % shareC).toString + "," + x(1) + y(1) + z(1) + "," + x(1) + "," + y(1) + "," + z(1) + "," + x(2) + "," + y(2) + "," + z(2)
				}
			}
		}	
		//creating the rdd of the three relations	
		val relationsList = mergedRelations.toList
		val relationABC_RDD = sc.parallelize(relationsList, totalReducers)
		
		//spliting into key value pairs
		val relationABC_RDD_Key_Value = relationABC_RDD.map(x => (x.split(",")(0).toInt, x))	
		//joining the relations	
		val joinR_ABC = relationR_RDD.join(relationABC_RDD_Key_Value)

		//Cleaning from parentheses and wrong generated joins beacause of the hashing
		val stringOfJoin = joinR_ABC.values.map(x => x.toString)		
		val cleanJoin = stringOfJoin.map(x => x.filterNot(toRemove))		
		val resultsCleaned = cleanJoin.map(x => x.split(",")).filter(x => x(1) == x(4)).map(x => (x(5) + "," + x(6) + "," + x(7) + "," + x(2) + "," + x(8) + "," + x(9) + "," + x(10)))	
		
		//saving the results
		resultsCleaned.saveAsTextFile(args(1) + "C-Results")	
	
		//calculating the elapsed time
		val timeC = System.currentTimeMillis -timeStart - timeA - timeB;
		println("The distribution of the reducers for each dimension table is A:" + shareA + " B: " + shareB + " C: " +shareC)
		println("Number of results of question C: " + resultsCleaned.count)
		println("Time passed in miliseconds: " + timeC + "\n")

		
		
		println("All OK")
	}
}



























