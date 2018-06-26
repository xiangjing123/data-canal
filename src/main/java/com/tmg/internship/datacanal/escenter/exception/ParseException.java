package com.tmg.internship.datacanal.escenter.exception;

/**
 * 解析异常
 *
 * @author xiangjing
 * @date 2018/5/5
 * @company 天极云智
 */
public class ParseException extends ESException{
    public ParseException(String msg) {
        super(msg);
    }

    public ParseException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public ParseException(Throwable cause) {
        super(cause);
    }
}
