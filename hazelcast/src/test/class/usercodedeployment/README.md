This directory contains resources for testing user code deployment use cases.

Contents:

- `ChildClass.class`, `ParentClass.class`: hierarchy of classes where `ChildClass extends ParentClass`.
- `IncrementingEntryProcessor.class`: an `EntryProcessor` for `<Integer, Integer>` entries that increments value by 1.
- `ShadedClasses.jar`: contains a class `com.hazelcast.core.HazelcastInstance` that defines a `main` method.
- `IncrementingEntryProcessor.jar`: contains `IncrementingEntryProcessor` class.
- `ChildParent.jar`: contains `ChildClass` and `ParentClass` as described above.
- `EntryProcessorWithAnonymousAndInner.jar`: contains class `EntryProcessorWithAnonymousAndInner`, to exercise loading classes with anonymous and named inner classes.

Note: unless package is explicitly specified, all classes described above reside in package `usercodedeployment`.