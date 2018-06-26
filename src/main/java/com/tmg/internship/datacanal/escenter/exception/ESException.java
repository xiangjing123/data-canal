package com.tmg.internship.datacanal.escenter.exception;

/**
 * 自定义的ES数据中心的异常
 *
 * @author xiangjing
 * @date 2018/5/5
 * @company 天极云智
 */
public class ESException extends RuntimeException {

    public ESException(String msg) {
        super(msg);
    }

    public ESException(String msg,Throwable cause) {
        super(msg,cause);
    }

    public ESException(Throwable cause) {
        super(cause);
    }

}
