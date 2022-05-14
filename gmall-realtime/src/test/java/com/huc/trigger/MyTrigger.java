package com.huc.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class MyTrigger extends Trigger<Object, TimeWindow> {

    private long delayTime;

    public MyTrigger(long delayTime) {
        this.delayTime = delayTime;
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        // 窗口[0,5] 其中来了条数据，注册定时器，也就是在5+2=7s的时候，到时间。
        ctx.registerProcessingTimeTimer(window.maxTimestamp() + delayTime);
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE; //评估窗口并发出结果。但是，不会清除窗口，但会保留所有元素。
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        // time 指的是定时器的时间
        if (time == window.maxTimestamp()) {
            ctx.deleteProcessingTimeTimer(window.maxTimestamp() + delayTime);
            return TriggerResult.FIRE;
        } else {
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
    }
}
