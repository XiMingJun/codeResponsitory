package com.zhiyou.bd23.weibo;

import java.nio.ByteBuffer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import hbaseDemo.com.zhiyou.bd23.HBaseUtils;



//负责各个表的rowkey 的组装
public class RowKeyHelper {

	/**
	 * 获取用户表的rowKey(40)
	 * @param: sequenceId(12) 唯一标识,递增之后，逆向处理
	 * @param: userName(17) 账户号
	 * @param: tel(11) 手机号
	 * */
	public static byte[] getUserTableRowKey(String userName,String tel) {
		
		//定义buffer,开辟内存空间，保存rowkey
		ByteBuffer rowkeyBuffer = ByteBuffer.allocate(40);
		
		String sequenceId = "";
		try {
			sequenceId = RowKeyHelper.getSequenceIdString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//处理sequenceId，定长反转
		while (sequenceId.length() < 12) {
			sequenceId = "0"+sequenceId;
		}
		rowkeyBuffer.put(StringUtils.reverse(sequenceId).getBytes());
	
		//账户号,17 位，长度不够，低位补0x1f
		ByteBuffer userNameBuffer = ByteBuffer.allocate(17);
		userNameBuffer.put(userName.getBytes());
		int num = userNameBuffer.remaining();
		byte byte1 = 0x1f;
		for (int i = 0; i < num; i++) {
			userNameBuffer.put(byte1);
		}
		rowkeyBuffer.put(userNameBuffer.array());
		
		//手机号，11位
		rowkeyBuffer.put(tel.getBytes());
		
		return rowkeyBuffer.array();
	}
	
	/**
	 * 获取微博表的rowkey
	 * @param: userId(40) 定长反转
	 * @param: timestamp(8) 时间戳反转
	 * 
	 * */
	public static byte[] getWeiboTableRowkey(String userId,long timestamp) {
		
		ByteBuffer rowkeyBuffer = ByteBuffer.allocate(48);
		
		//userId
		while (userId.length() < 40) {
			userId = "0" + userId;
		}
		rowkeyBuffer.put(StringUtils.reverse(userId).getBytes());
		
		//时间戳反转
		long revDate = Long.MAX_VALUE - timestamp;
		rowkeyBuffer.put(Bytes.toBytes(revDate));
		
		
		return rowkeyBuffer.array();
	}
	
	/**
	 * 获取微博操作表的rowkey
	 * @param: weibiId(40) 定长反转
	 * @param: userId(40) 定长反转
	 * @param: timestamp(8) 时间戳反转
	 * 
	 * */
	public static byte[] getWeiBoOperRowkey(String weibiId,String userId,long timestamp) {
		
		ByteBuffer rowkeyBuffer = ByteBuffer.allocate(88);
		//weiboId
		while (weibiId.length() < 40) {
			weibiId = "0" + weibiId;
		}
		rowkeyBuffer.put(StringUtils.reverse(weibiId).getBytes());
		
		//userId
		while (userId.length() < 40) {
			userId = "0" + userId;
		}
		rowkeyBuffer.put(StringUtils.reverse(userId).getBytes());
		
		//时间戳反转
		long revDate = Long.MAX_VALUE - timestamp;
		rowkeyBuffer.put(Bytes.toBytes(revDate));
		
		return rowkeyBuffer.array();
	}
	
	/**
	 * 获取唯一标识符(自增)
	 * @throws Exception 
	 * */
	public static String getSequenceIdString() throws Exception {
		
		long amount = 1;//sequence 表每次自增一
		Table sequenceTable = HBaseUtils.getTable("bd23:sequence");
		//返回新插入放入值
		long index = sequenceTable.incrementColumnValue("sequence".getBytes(), "i".getBytes(), "seq".getBytes(), amount);		
		return String.valueOf(index);
		
	}
	
	
}
