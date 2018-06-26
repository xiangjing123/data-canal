package com.tmg.internship.datacanal.escenter.exception;

/**
 * 执行时异常
 *
 * @author xiangjing
 * @date 2018/5/5
 * @company 天极云智
 */
public class ExecuteException extends ESException {


    public ExecuteException(String msg) {
        super(msg);
    }

    public ExecuteException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public ExecuteException(Throwable cause) {
        super(cause);
    }
}
