package com.artos.inttests.core.sim.intercept;

import com.artos.inttests.core.sim.api.ISimGroup;
import com.artos.inttests.core.sim.impl.SimGroup;
import com.exametrika.api.instrument.config.ClassFilter;
import com.exametrika.api.instrument.config.InstrumentationConfiguration;
import com.exametrika.api.instrument.config.InterceptPointcut;
import com.exametrika.api.instrument.config.InterceptPointcut.Kind;
import com.exametrika.api.instrument.config.MemberFilter;
import com.exametrika.api.instrument.config.Pointcut;
import com.exametrika.api.instrument.config.QualifiedMethodFilter;
import com.exametrika.common.config.common.RuntimeMode;
import com.exametrika.common.utils.Enums;
import com.exametrika.common.utils.Exceptions;
import com.exametrika.common.utils.Pair;
import com.exametrika.impl.instrument.StaticClassTransformer;
import com.exametrika.impl.instrument.StaticInterceptorAllocator;
import com.exametrika.spi.instrument.config.StaticInterceptorConfiguration;
import com.exametrika.tests.instrument.instrumentors.TestClassLoader;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SimGroupInterceptors {
    private int nextIndex = 1;
    private static StaticInterceptorAllocator interceptorAllocator = new StaticInterceptorAllocator();

    public static StaticInterceptorAllocator getInterceptorAllocator() {
        return interceptorAllocator;
    }

    public ISimGroup createGroup(List<Pair<String, Class>> interceptors) {
        Set<Pointcut> pointcuts = new HashSet<>();
        for (Pair<String, Class> pair : interceptors)
            pointcuts.add(createPointcut(pair.getKey(), pair.getValue()));

        File tempDir = new File(System.getProperty("java.io.tmpdir"));

        StaticClassTransformer classTransformer = new StaticClassTransformer(interceptorAllocator, getClass().getClassLoader(),
                new InstrumentationConfiguration(RuntimeMode.DEVELOPMENT, pointcuts,
                        true, new File(tempDir, "transform"), Integer.MAX_VALUE), new File(tempDir, "transform"));

        List<String> basePackages = Arrays.asList(
                "com.exametrika.impl.raft", "com.exametrika.api.raft", "com.exametrika.spi.raft",
                "com.exametrika.inttests.raft.sim.model", "com.exametrika.inttests.raft.sim.impl");
        TestClassLoader classLoader = new TestClassLoader(basePackages, classTransformer, true);

        try {
            return (ISimGroup) classLoader.loadClass(SimGroup.class.getName()).getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            return Exceptions.wrapAndThrow(e);
        }
    }

    private Pointcut createPointcut(String methodFilter, Class clazz) {
        int pos = methodFilter.lastIndexOf(".");
        String classFilter = methodFilter.substring(0, pos);
        String memberFilter = methodFilter.substring(pos + 1);
        QualifiedMethodFilter filter = new QualifiedMethodFilter(new ClassFilter(classFilter), new MemberFilter(memberFilter));
        return new InterceptPointcut("test" + nextIndex++, filter, Enums.of(Kind.ENTER),
            new StaticInterceptorConfiguration(clazz), true, false, 0);
    }
}
