package model;

import java.util.Map;

public class Row {
	private byte[] rowKey;
	private Map<byte[],Map<byte[],byte[]>> rowData;
	
	public Row() {
		super();
	}

	public Row(byte[] rowKey, Map<byte[], Map<byte[], byte[]>> rowData) {
		super();
		this.rowKey = rowKey;
		this.rowData = rowData;
	}

	public byte[] getRowKey() {
		return rowKey;
	}

	public void setRowKey(byte[] rowKey) {
		this.rowKey = rowKey;
	}

	public Map<byte[], Map<byte[], byte[]>> getRowData() {
		return rowData;
	}

	public void setRowData(Map<byte[], Map<byte[], byte[]>> rowData) {
		this.rowData = rowData;
	}
	
}
