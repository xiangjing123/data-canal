package com.tmg.internship.datacanal.escenter.parser;

import com.tmg.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 解析器工厂类
 *
 * @author xiangjing
 * @date 2018/5/7
 * @company 天极云智
 */
public class ParserFactory {

    public static final Logger logger = LoggerFactory.getLogger(ParserFactory.class);

    public static EventParser getParser(String event) {
        if (StringUtils.isEmpty(event)) {
            throw  new RuntimeException("Accept The MQ Message Is Not Recognition");
        } else {
            if (event.endsWith(Event.DELETE.name()) || event.endsWith(Event.UPDATE.name()) || event.endsWith(Event.INSERT.name())) {
                return new BaseEventParser();
            } else {
                logger.error("PASER Cant't parse the MQ data");
                return null;
            }
        }

    }
}
