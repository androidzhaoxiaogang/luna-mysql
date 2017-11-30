package luna.app;

import luna.common.Bootstrap;
/**
 * 
* Copyright: Copyright (c) 2017 XueErSi
*
* @version v1.0.0
* @author GaoXing Chen
*
* Modification History:
* Date         Author          Version			Description
*---------------------------------------------------------*
* 2017年11月29日     GaoXing Chen      v1.0.0		  添加注释
 */
public class App {
	public static void main( String[] args ){
        Bootstrap bootstrap = new Bootstrap("src/conf/example.yml");
		bootstrap.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
               bootstrap.stop();
            }
        });
	}
}
