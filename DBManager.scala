import java.sql._
import scala.util._

object Ops {
  implicit def whileLoop(cond : =>Boolean, block : =>Unit) : Unit =
	  if(cond) {
	    block
	    whileLoop(cond, block)
	 } 
}

import Ops._

sealed class DBTableDef(val tablename:String,val schema:Map[String,(String,Int)])
sealed class DBFetchedData(val query:String,val columnsCount:Int,val rowSet:RsIterator)

sealed class RsIterator(rs: ResultSet) extends Iterator[ResultSet] {
    def hasNext: Boolean = rs.next
    def next: ResultSet = rs
    def close = rs.close
}

abstract class BasicRDBManager{
  private val dbCon = PostgresqlConnection("hostname","port","db_name","username")

  protected def getSchema(tablename:String):DBTableDef={
    val conn = dbCon.conn
    try {
	    val columns = conn.getMetaData.getColumns(null, null,tablename.toLowerCase,null)    
	    val schema = scala.collection.mutable.Map[String,(String,Int)]()    
	    columns.next match {
	      case false => 
	        new DBTableDef("",Map())
	      case true  =>        
	        whileLoop(columns.next, {schema.put(columns.getString("COLUMN_NAME"),(columns.getString("TYPE_NAME"),columns.getString("ORDINAL_POSITION").toInt))})	        
	        new DBTableDef(tablename,schema.map(kv => (kv._1,(kv._2._1,kv._2._2))).toMap)
	    }
    }
    catch {
       case e:Throwable => 
        println("Failed to fetch -> "+ e.printStackTrace)
        new DBTableDef(tablename,Map())
    }
    finally {
        conn.close
    }
  }
  
  protected def fetch(query:String):DBFetchedData={
    val conn = dbCon.conn
    val stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_FORWARD_ONLY)
    try {  
    val rs = stmt.executeQuery(query) 
    new DBFetchedData(query,rs.getMetaData.getColumnCount,new RsIterator(rs))
    }
    catch {
      case e:Throwable => 
        println("Failed to fetch -> "+ e.printStackTrace)
        new DBFetchedData(query,0,new RsIterator(stmt.getResultSet))
    }
    finally{
        conn.close
    }    
  }
  
  protected def execute(statement:String):Boolean = {
    val conn = dbCon.conn
    try{
    	val stmt = conn.createStatement
    	stmt.executeUpdate(statement)
    	true
    } 
    catch {
       case e:Throwable => 
        println("Failed to fetch -> "+ e.printStackTrace)
        false
    }
    finally {
        conn.close
    }
  }
    
  protected def executeBatch(data:Map[String,String],tableDef:DBTableDef):Boolean ={
    val conn = dbCon.conn
    conn.setAutoCommit(false)
    try{
    	val sql = "INSERT INTO " + tableDef.tablename + "("+ tableDef.schema.keys.mkString(",") + ")" + " VALUES (" + tableDef.schema.map(_ => "?").mkString(",") + ")"
    	val pstmt = conn.prepareStatement(sql)
    	tableDef.schema.zipWithIndex.foreach(col => {
    	  val parameterIndex = col._2 + 1
    	  val dataType = col._1._2._1
    	  val colName  = col._1._1   	  
    	  dataType match {
    	     case "bool" =>     	        
    	        pstmt.setBoolean(parameterIndex,getBoolType(data.get(colName)))
    	     case "text" =>
    	        pstmt.setString(parameterIndex, data.get(colName).getOrElse(""))
    	     case "varchar" =>
    	        pstmt.setString(parameterIndex, data.get(colName).getOrElse(""))
    	     case "int4" =>
    	        pstmt.setInt(parameterIndex, data.get(colName).getOrElse("0").toInt)
    	     case "date"  =>
    	        pstmt.setDate(parameterIndex, java.sql.Date.valueOf(data.get(colName).getOrElse("0000-00-00")))    	     
    	  }    	     	 
    	})
    	pstmt.addBatch
    	pstmt.execute
    	conn.commit
    	true
    } 
    catch {
       case e:Throwable => 
        println("Failed to fetch -> "+ e.printStackTrace)
        false
    }
    finally {
        conn.close
    }
  }
  
  protected def getBoolType(data:Any):Boolean = {
      data match {
        case "true" => true
        case _      => false
      }   
  }
}
