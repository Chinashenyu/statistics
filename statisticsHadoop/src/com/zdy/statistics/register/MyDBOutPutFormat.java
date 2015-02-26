package com.zdy.statistics.register;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDBOutPutFormat<K  extends DBWritable, V> extends OutputFormat<K, V> {

	public class MyDBRecorderWriter extends RecordWriter<K, V>{

		private Connection connection;
		private PreparedStatement statement;
		
		public MyDBRecorderWriter() {}
		
		public MyDBRecorderWriter(Connection connection, PreparedStatement statement) throws SQLException {
			this.connection = connection;
			this.statement = statement;
			this.connection.setAutoCommit(false);
		}


		@Override
		public void write(K key, V value) throws IOException, InterruptedException {
			try {
				key.write(statement);
				statement.addBatch();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			try {
				statement.executeBatch();
				connection.commit();
			} catch (SQLException e) {
				try {
					connection.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}finally{
				try {
					statement.close();
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	public String createSQL(String tableName,String[] fields){
		if(fields == null) {
		      throw new IllegalArgumentException("Field names may not be null");
		    }

		    StringBuilder query = new StringBuilder();
		    query.append("INSERT INTO ").append(tableName);

		    if (fields.length > 0 && fields[0] != null) {
		      query.append(" (");
		      for (int i = 0; i < fields.length; i++) {
		        query.append(fields[i]);
		        if (i != fields.length - 1) {
		          query.append(",");
		        }
		      }
		      query.append(")");
		    }
		    query.append(" VALUES (");

		    for (int i = 0; i < fields.length; i++) {
		      query.append("?");
		      if(i != fields.length - 1) {
		        query.append(",");
		      }
		    }
		    query.append(") ");
		    query.append(" ON DUPLICATE KEY UPDATE new_device =?");
		    System.out.println("query sql :"+ query.toString());
		    return query.toString();
	}
	
	public static void setOutput(Job job,String tableName,String...fieldNames){
		DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
		dbConf.setOutputTableName(tableName);
		dbConf.setOutputFieldNames(fieldNames);
	}
	
	@Override
	public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
		
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(taskAttemptContext),
				taskAttemptContext);
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		MyDBRecorderWriter myDBRecorderWriter = null;
		DBConfiguration dbConf = new DBConfiguration(taskAttemptContext.getConfiguration());
		String table = dbConf.getOutputTableName();
		String[] fields = dbConf.getOutputFieldNames();
		System.out.println("table fields : "+table +" , "+fields);
		try {
			Connection conn = dbConf.getConnection();
			PreparedStatement statement = conn.prepareStatement(createSQL(table,fields));
			myDBRecorderWriter = new MyDBRecorderWriter(conn, statement);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return myDBRecorderWriter;
	}

}
