package com.artos.inttests.shards.sim.model;

import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;

public interface ISimDataSpaceQueryChannel<K> {
    void query(K key, ByteArray request, ICompletionHandler<ByteArray> responseHandler);
}
