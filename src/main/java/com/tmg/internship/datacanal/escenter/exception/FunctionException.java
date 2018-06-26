package com.tmg.internship.datacanal.escenter.exception;

/**
 * 函数异常
 *
 * @author xiangjing
 * @date 2018/6/12
 * @company 天极云智
 */
public class FunctionException extends  ESException {

    public FunctionException(String msg) {
        super(msg);
    }

    public FunctionException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public FunctionException(Throwable cause) {
        super(cause);
    }
}
