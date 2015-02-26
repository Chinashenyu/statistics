package com.zdy.statistics.register;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class RegisterVo implements Writable,DBWritable{

	private int newDevice;
	private String date;
	
	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		this.newDevice = resultSet.getInt(0);
		this.date = resultSet.getString(1);
	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {
		statement.setInt(1, newDevice);
		statement.setString(2, date);
		statement.setInt(3, newDevice);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.newDevice = dataInput.readInt();
		this.date = dataInput.readUTF();
	}

	@Override
	public void write(DataOutput dataOutPut) throws IOException {
		dataOutPut.writeInt(newDevice);
		dataOutPut.writeUTF(date);
	}

	public int getNewDevice() {
		return newDevice;
	}

	public void setNewDevice(int newDevice) {
		this.newDevice = newDevice;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}
}
