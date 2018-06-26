package com.tmg.internship.datacanal;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

/**
 * cli方式的应用启动入口
 *
 * @author Paul
 * @company 天极云智
 * @date 2018/1/17
 **/
// 默认扫描Application所在包路径，也可用该注解自定义扫描路径下的所有spring组件
//@ComponentScan(basePackages={"com.tmg.internship.datacanal.*","com.tmg.dsp.porter.client.demo.springboot"})
@Configuration
//@ImportResource(locations={"classpath:applicationBean.xml"})
//@ComponentScan(basePackages={"com.tmg.internship.datacanal.*"})
@SpringBootApplication
public class CanalBootApplication{

    public static void main(String[] args) {

       //第一种快捷运行方式
//        final ApplicationContext context = SpringApplication.run(CanalBootApplication.class, args);


         //第二种设置一些属性后再启动的方式
        SpringApplication application = new SpringApplication(CanalBootApplication.class);
        application.setBannerMode(Banner.Mode.OFF);
        application.setWebEnvironment(false);
        final ApplicationContext context = application.run(args);

    }

}
