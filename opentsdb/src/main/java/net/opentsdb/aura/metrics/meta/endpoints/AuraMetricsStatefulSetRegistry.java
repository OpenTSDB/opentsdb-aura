package net.opentsdb.aura.metrics.meta.endpoints;

import net.opentsdb.aura.metrics.meta.endpoints.impl.Component;

public class AuraMetricsStatefulSetRegistry extends BaseStatefulSetRegistry {
    private static final String pod_name_prefix = "%s-aura-metrics-%s%s";
    private static final String TYPE = "AuraMetricsStatefulSetRegistry";
    private static final String COMPONENT_NAME = "aura-metrics";

    @Override
    protected String getComponentName() {
        return COMPONENT_NAME;
    }

    @Override
    protected String getPrefix(String namespace, Component component, String replica, long epoch) {
        final int epochPrefix = getEpochPrefix(epoch, component.getEpochLength());
        return String.format(
                pod_name_prefix,
                namespace,
                epochPrefix,
                replica
        );
    }

    private int getEpochPrefix(long epoch, int epochLength) {
        //Get index of hour in the day.
        final long epochHr = epoch - epoch % 3600;
        final long epochDay = epoch - epoch % 86400;
        return (int)(epochHr - epochDay) / 3600;
    }

    @Override
    public String type() {
        return TYPE;
    }
}
