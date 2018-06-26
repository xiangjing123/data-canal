package com.tmg.internship.datacanal.escenter.exception;

/**
 * es 配置文件解析异常
 *
 * @author xiangjing
 * @date 2018/6/5
 * @company 天极云智
 */
public class ESConfigParseException extends ESException {

    public ESConfigParseException(String msg) {
        super(msg);
    }

    public ESConfigParseException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public ESConfigParseException(Throwable cause) {
        super(cause);
    }
}
