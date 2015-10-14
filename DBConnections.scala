package com.martines.model.db

import java.sql._
import java.util.Properties

sealed class PostgresqlConnection(url:String,username:String,password:String="") extends BasicDatabaseConnection{  
   def conn:java.sql.Connection= {
     val props = new Properties()
     props.setProperty("user",username)
     props.setProperty("password",password)   
     DriverManager.getConnection(url,props)
   }     
}

object PostgresqlConnection{
  def apply(host:String,port:String,databaseName:String,username:String,password:String=""):BasicDatabaseConnection={         
    val url = "jdbc:postgresql://"+host+":"+port+"/"+databaseName    
    new PostgresqlConnection(url,username,password)    
  }   
}