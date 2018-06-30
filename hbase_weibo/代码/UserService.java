/**  
 * @Title: UserService.java
 * @Package com.zhiyou.bd23.weibo
 * @Description: TODO：
 * @author Administrator
 * @date 2018年6月30日
 * @version V1.0  
 * */
package com.zhiyou.bd23.weibo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import hbaseDemo.com.zhiyou.bd23.HBaseUtils;

/**
 * @ClassName: UserService
 * @Description: 用户相关操作
 * @author Administrator
 * @date 2018年6月30日
 *
 * */
public class UserService {

	/**
	 * @Title: registUser
	 * @Description: 注册用户
	 * @param: user_name 用户名
	 * @param: password 密码
	 * @param: tel    手机号
	 * @return void    返回类型
	 * @throws Exception 
	 * */  
	public static void registUser(String user_name,String password,String tel) throws Exception  {
		
		//首先查询该用户是否存在，
		System.out.println("查询该用户是否存在...");
		
		if (!isUserExists(user_name)) {
			
			System.out.println("开始注册....");
			//没有该用户信息，可以注册
			byte[] userRowKey = RowKeyHelper.getUserTableRowKey(user_name, tel);
			
			Put put = new Put(userRowKey);
			put.addColumn("i".getBytes(), "user_name".getBytes(), user_name.getBytes());
			put.addColumn("i".getBytes(), "password".getBytes(), password.getBytes());
			put.addColumn("i".getBytes(), "tel".getBytes(), tel.getBytes());
			
			Table userTable = HBaseUtils.getTable("bd23:w_user");
			try {
				userTable.put(put);
				System.out.println("--->注册成功！");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("xxx-->注册失败");
			}	
		}
		else {
			System.out.println("该用户已存在");
		}
		
		
		
	}
	/**
	 * @throws Exception 
	 * @Title: loginUser
	 * @Description: 登陆
	 * @param user_name 用户名
	 * @param password  密码
	 * @return void    返回类型
	 * @throws
	 * */  
	public static void loginUser(String user_name,String password) throws Exception {
		
		System.out.println("登录用户...");
		List<Cell> cells = getUserInfoCells(user_name);
		if (cells.size() <= 0) {
		
			System.out.println("该用户不存在");
		}
		for (Cell cell : cells) {
			byte[] row = CellUtil.cloneRow(cell);
			byte[] family = Arrays.copyOfRange(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyOffset()+cell.getFamilyLength());
			byte[] qualify = CellUtil.cloneQualifier(cell);
			long timestamp = cell.getTimestamp();
			byte[] value = CellUtil.cloneValue(cell);
			Type type = cell.getType();
			
			if ("password".equals(Bytes.toString(qualify))) {
				//密码正确

				if (password.equals(Bytes.toString(value))) {
					System.out.println("----------->>密码正确");
				}
				else {
					//密码错误
					System.out.println("用户名或密码错误");
				}
			}
			System.out.println("rowkey:"+Bytes.toString(row)+
					"|family:"+Bytes.toString(family)+
					"|qualify:"+Bytes.toString(qualify)+
					"|timestamp:"+timestamp+
					"|value:"+Bytes.toString(value)+
					"|vtype:"+type.name()
			);

		}	
	}
	/**
	 * @Title: isUserExists
	 * @Description: 查询该用户是否已存在
	 * @param user_name
	 * @return boolean    返回类型
	 * @throws Exception
	 * */  
	private static boolean isUserExists(String user_name) throws Exception {
		// TODO Auto-generated method stub

		//查到有数据，说明该用户存在
		return getUserInfoCells(user_name).size() > 0;
		
	}
	
	/**
	 * @throws IOException 
	 * @Title: getUserInfoCells
	 * @Description: 获取用户信息
	 * @param user_name 用户名
	 * @return    参数
	 * @return List<Cell>    返回类型
	 * @throws
	 * */  
	public static List<Cell> getUserInfoCells(String user_name) throws IOException {
		
		//账户号,17 位，长度不够，低位补0x1f
		ByteBuffer userNameBuffer = ByteBuffer.allocate(17);
		userNameBuffer.put(user_name.getBytes());
		int num = userNameBuffer.remaining();
		byte blank = 0x1f;
		for (int i = 0; i < num; i++) {
			userNameBuffer.put(blank);
		}
		
		//comparaValue
		ByteBuffer compareValue = ByteBuffer.allocate(40);
		compareValue.put("xxxxxxxxxxxx".getBytes());//12位
		compareValue.put(userNameBuffer.array());//17 位用户账号
		compareValue.put("xxxxxxxxxxx".getBytes());//11位
		
		//comparePosition
		ByteBuffer comparePosition = ByteBuffer.allocate(40);
		byte zero = 0x00;
		byte one = 0x01;
		for(int i=0;i<12;i++){
			comparePosition.put(one);
		}
		for(int i=0;i<17;i++){
			comparePosition.put(zero);
		}
		for(int i=0;i<11;i++){
			comparePosition.put(one);
		}
		
		//Pair
		Pair<byte[], byte[]> fuzzyParam = new Pair<byte[], byte[]>();
		fuzzyParam.setFirst(compareValue.array());
		fuzzyParam.setSecond(comparePosition.array());
		
		List<Pair<byte[], byte[]>> fuzzyParamList = new ArrayList<Pair<byte[], byte[]>>();
		fuzzyParamList.add(fuzzyParam);
		
		Filter filter = new FuzzyRowFilter(fuzzyParamList);
		Scan scan = new Scan();
		scan.setFilter(filter);
		
		Table userTable = HBaseUtils.getTable("bd23:w_user");
		ResultScanner resultScanner =  userTable.getScanner(scan);

		Result result = resultScanner.next();
		
		if (result == null) {
			return new ArrayList<Cell>();
		}
		return result.listCells();
		
	}
	
	
	/**
	 * @Title: focus
	 * @Description: 关注某人
	 * @param otherId 关注的别人userId
	 * @param myId    自己的Id
	 * @param focus   true 关注，false 取消关注
	 * @return void    返回类型
	 * @throws
	 * */  
	public static void focus(String otherId,String myId,Boolean fouse) {
		
		//bd23:w_user 表插入数据 列簇 c，f+otherId,u+myId
		Table userTable = HBaseUtils.getTable("bd23:w_user");
		Put put = new Put(myId.getBytes());
		
		put.addColumn("c".getBytes(), ("f"+otherId).getBytes(), fouse?"1".getBytes():"0".getBytes());
		put.addColumn("c".getBytes(), ("u"+myId).getBytes(), fouse?"1".getBytes():"0".getBytes());

		try {
			userTable.put(put);
			System.out.println("关注成功");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("关注失败");
		}
		
	}
	
	/**
	 * @Title: getFocusInfo
	 * @Description: 查询关注的其他人
	 * @param userId    要查询的人的userId
	 * @return void    返回类型
	 * @throws
	 * */  
	public static void getFocusInfo(String userId) {
		
		//查询bd23:w_user c列簇的所有数据,以f 开头
		System.out.println("查询关注了谁...");
		Table userTable = HBaseUtils.getTable("bd23:w_user");
		Get get = new Get(userId.getBytes());
		
		try {
			Result result = userTable.get(get);
			if (result == null) {
				System.out.println("没有关注的人的信息");
			}
			while (result.advance()) {
				Cell cell = result.current();
				String row = Bytes.toString(CellUtil.cloneRow(cell));
				String family = Bytes.toString(CellUtil.cloneFamily(cell));
				String qualify = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				if (qualify.startsWith("f")) {
					System.out.println("rowkey:"+row+
							"| family:"+family+
							"| qualify:"+qualify+
							"| value:"+value);	
				}
			
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * @Title: getFocusedInfo
	 * @Description: 查询被谁关注
	 * @param userId    要查询的人的userId
	 * @return void    返回类型
	 * @throws
	 * */  
	public static void getFocusedInfo(String userId) {
		
		System.out.println("查询被谁关注...");
		//查询bd23:w_user c列簇的所有数据 以f开头
		Table userTable = HBaseUtils.getTable("bd23:w_user");
		try {
			
			//全表扫描  c列簇
			ResultScanner resultScanner = userTable.getScanner("c".getBytes());

			Result result = resultScanner.next();
			
			if (result == null) {
				System.out.println("还没有人关注你哦");
			}
			while (result.advance()) {
				Cell cell = result.current();
				String row = Bytes.toString(CellUtil.cloneRow(cell));
				String family = Bytes.toString(CellUtil.cloneFamily(cell));
				String qualify = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				if (qualify.equals("f"+userId)) {
					System.out.println("rowkey:"+row+
							"| family:"+family+
							"| qualify:"+qualify+
							"| value:"+value);	
				}
				//这里也有可能没人关注，只存在关注别人的情况
			
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
