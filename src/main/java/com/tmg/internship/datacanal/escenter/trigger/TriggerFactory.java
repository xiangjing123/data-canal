package com.tmg.internship.datacanal.escenter.trigger;

import com.tmg.internship.datacanal.escenter.parser.Event;

/**
 * 触发器工厂类
 *
 * @author xiangjing
 * @date 2018/6/1
 * @company 天极云智
 */
public class TriggerFactory {


    private static ESTrigger updateTrigger;

    private static ESTrigger deleteTrigger;

    private static ESTrigger insertTrigger;

    public static ESTrigger getESTrigger(Event event) {
        if (event == Event.DELETE) {
            return getDeleteTrigger();
        } else if (event == Event.UPDATE) {
            return getUpdateTrigger();
        } else {
            return getMergeTrigger();
        }
    }

    private static ESTrigger getUpdateTrigger() {
        if (updateTrigger == null) {
            updateTrigger = new UpdateEventTrigger();
        }
        return updateTrigger;

    }

    private static ESTrigger getMergeTrigger() {
        if (insertTrigger == null) {
            insertTrigger = new MergeEventTrigger();
        }
        return insertTrigger;

    }

    private static ESTrigger getDeleteTrigger() {
        if (deleteTrigger == null) {
            deleteTrigger = new DeleteEventTrigger();
        }
        return deleteTrigger;

    }
}
