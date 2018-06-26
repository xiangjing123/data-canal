package com.tmg.internship.datacanal.escenter.exception;

/**
 * 映射异常
 *
 * @author xiangjing
 * @date 2018/5/5
 * @company 天极云智
 */
public class MappingException extends ESException {
    public MappingException(String msg) {
        super(msg);
    }

    public MappingException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public MappingException(Throwable cause) {
        super(cause);
    }
}
