package com.dynamodb
import org.apache.hadoop.io.Text;

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
/* Importing DynamoDBInputFormat and DynamoDBOutputFormat */
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.LongWritable
import scala.util.matching.Regex
import java.util.HashMap

import com.amazonaws.services.dynamodbv2.model.AttributeValue
object SparkDynamo {
  
    def main(args:Array[String]){
    
  val conf = new SparkConf()
             .setMaster("local")
             .setAppName("spark-dynamodb")
            
            
  val sc = new SparkContext(conf)
  
  val sqlContext = new SQLContext(sc)
             

import com.amazonaws.services.dynamodbv2.document.{ DynamoDB, Table, Item }
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.auth.{AWSCredentialsProvider, AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.client.builder.AwsClientBuilder;
//val credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials("AKIA5RW5H72QSWHWMMQ2", "T1BaoBVwBndCSNoNiOYXARpry2IeDVAp6qfCSVR1"))

val airportdf = sqlContext.read.format("CSV").option("header", "true").option("inferSchema", "true").load("C:\\Users\\Tesseract\\Downloads\\airport.csv")

airportdf.rdd.foreachPartition(iter => {
val client = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("dynamodb.us-east-1.amazonaws.com", "us-east-1"))
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("xxxx", "xxxx")))
            .build()

 val dynamoDB = new DynamoDB(client);     
 
dynamoDB.getTable("Airport") 
val dynamoTable = dynamoDB.getTable("Airport") 
def putItem(name: String, country: String, areacode: Int, code : String): Unit = {
    val item = new Item().withPrimaryKey("name", name).withString("country", country).withNumber("areacode", areacode).withString("code", code)
    dynamoTable.putItem(item)
}

 // iter.foreach(row => putItem(row.getS
  iter.foreach(row => putItem(row.getString(0), row.getString(1),row.getInt(2),row.getString(3) ) )
})









    }
  
}