package com.zhiyou.bd23.weibo;



/*
 * create 'bd23:w_user','i','c' //用户表
 * create 'bd23:weibo_info','i','o' //微博信息表
 * create 'bd23:weibo_oper','i' //微博操作表
 * create 'bd23:sequence','i'
 * */
public class WeiBoAPI {

	 

	
	public static void main(String[] args) throws Exception {

		//注册
//		UserService.registUser("wangjian", "12345", "13168734456");
//		UserService.registUser("yuyu", "12345", "1221312312");
		
		//登录
//		UserService.loginUser("wangjian", "12345");
//		UserService.loginUser("yuyu", "12345");
		//关注
//		UserService.focus("210000000000yuyu1221312312", "110000000000wangjian13168734456",true);
	
		//关注的人 的信息
//		UserService.getFocusInfo("110000000000wangjian13168734456");
		
		//查询被谁关注
		UserService.getFocusedInfo("210000000000yuyu1221312312");
		
		
	}
}
