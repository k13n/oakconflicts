# Oak-Conflicts

This is some Java code to provoke conflicts in a Jackrabbit Oak repository due to concurrent updates.

- Class `LocalConflictExample` creates a conflict on node `/a` by two sessions originating from the same cluster node.
- Class `DistributedConflictExample` creates a conflict on node `/a` by two sessions originating from two different cluster nodes.
