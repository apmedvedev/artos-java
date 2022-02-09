package com.artos.inttests.shards.sim.model;

import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;

public interface ISimDataSpacePublishChannel<K> {
    void publish(K key, ByteArray value, ICompletionHandler<ByteArray> commitHandler);
}
