package com.tmg.internship.datacanal.escenter.exception;

/**
 * es触发器异常
 *
 * @author xiangjing
 * @date 2018/6/5
 * @company 天极云智
 */
public class ESTriggerException extends ESException {
    public ESTriggerException(String msg) {
        super(msg);
    }

    public ESTriggerException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public ESTriggerException(Throwable cause) {
        super(cause);
    }
}
