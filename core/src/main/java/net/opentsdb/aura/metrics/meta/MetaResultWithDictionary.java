package net.opentsdb.aura.metrics.meta;

public interface MetaResultWithDictionary extends MetaTimeSeriesQueryResult {

    public interface Dictionary {
        void put(final long id, final byte[] value);

        byte[] get(final long id);

        int size();

        // Unorthodox way of doing things.
        // Helps with not having to manage iterators.
        // There may be a better way to do this.
        void mergeInto(Dictionary dictionary);

    }

    Dictionary getDictionary();
}
