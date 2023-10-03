This directory contains resources for testing user code deployment use cases.

Contents:

- `ChildClass.class`, `ParentClass.class`: hierarchy of classes where `ChildClass extends ParentClass`.
- `IncrementingEntryProcessor.class`: an `EntryProcessor` for `<Integer, Integer>` entries that increments value by 1.
- `ShadedClasses.jar`: contains a class `com.hazelcast.core.HazelcastInstance` that defines a `main` method.
- `IncrementingEntryProcessor.jar`: contains `IncrementingEntryProcessor` class.
- `ChildParent.jar`: contains `ChildClass` and `ParentClass` as described above.
- `EntryProcessorWithAnonymousAndInner.jar`: contains class `EntryProcessorWithAnonymousAndInner`, to exercise loading classes with anonymous and named inner classes.
- `LowerCaseValueEntryProcessor`, `UpperCaseValueEntryProcessor`: `EntryProcessor` who adjust the case of the value, with deliberately overlapping class names.

Note: unless package is explicitly specified, all classes described above reside in package `usercodedeployment`.

To generate a new `.class` from from an existing `.java` file, run something like:
```
javac --release 11 YourJavaClass.java -cp "~/.m2/repository/com/hazelcast/hazelcast/5.3.2/hazelcast-5.3.2.jar"
```