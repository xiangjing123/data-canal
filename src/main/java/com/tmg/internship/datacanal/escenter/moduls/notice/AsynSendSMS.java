package com.tmg.internship.datacanal.escenter.moduls.notice;

import com.tmg.utils.SendSMS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 短信发送
 *
 * @author xiangjing
 * @date 2018/5/17
 * @company 天极云智
 */
public class AsynSendSMS {

    private static final Logger logger = LoggerFactory.getLogger(AsynSendSMS.class);

    private static ExecutorService executors ;

    private static Runnable runnable;

    /**
     * 短信默认最多发送多少次
     */
    public static int default_max_send_num = 5;

    /**
     * 最多发送次数
     */
    private int maxSendNum = default_max_send_num;
    /**
     * 电话
     */
    private String phone;

    /**
     * 管理员
     */
    private String admin;

    public AsynSendSMS() {
    }

    public AsynSendSMS(int maxSendNum,String phone, String admin) {
        this.phone = phone;
        this.maxSendNum = maxSendNum;
        this.admin = admin;
    }

    static {
        if(executors == null ){
            executors=Executors.newSingleThreadExecutor();
        }
    }

    private  Runnable getRunnable(String msg){

        if(runnable == null){
            runnable = () ->{
                Thread.currentThread().setName("AsynSendMessage");
                boolean bl= true;
                for(int i=0;i<maxSendNum;i++){
                    try {
                        bl =SendSMS.sendMsg(phone,msg);
                        if(bl){
                            break;
                        }
                    }catch (Exception ex){
                        if( i == maxSendNum){
                            logger.error(msg+",{},消息发送失败,请"+admin+"管理员尽快查看",ex);
                        }
                    }
                }
            };
        }
        return runnable;
    }
    /**
     * 发送消息
     * @param msg
     */
    public void sendMsg(String msg){
        msg=admin+"同学，你有ES数据中心的索引有变化任务的实习总结未提交，请及时提交。";
      //  msg = "管理员,"+admin +"您好"+"! ES 数据中心"+msg+"索引有变化，请注意查收";
        executors.execute(getRunnable(msg));
    }

    public void destory(){
        this.executors.shutdown();
    }

    public int getMaxSendNum() {
        return maxSendNum;
    }

    public void setMaxSendNum(int maxSendNum) {
        this.maxSendNum = maxSendNum;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getAdmin() {
        return admin;
    }

    public void setAdmin(String admin) {
        this.admin = admin;
    }
}
