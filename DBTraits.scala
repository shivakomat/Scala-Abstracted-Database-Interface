package com.martines.model.db

import com.martines.model.util._

trait BasicDatabaseConnection{  
  def conn:java.sql.Connection 
}

trait DatabaseManager{ 
  type T  
  def read(property:String):Option[T]
  def insert(item:T):Boolean
  def delete(property:String):Boolean
  def update(item:T):Boolean
}




