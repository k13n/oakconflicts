# Oak-Conflicts

This is some Java code to provoke conflicts in a Jackrabbit Oak repository due to concurrent updates.

- Class `LocalConflictExample` creates a conflict on node `/a` by two sessions originating from the same cluster node. The expected behavior is that `session1` succeeds and `session2` fails with a `CommitFailedException`. The node looks like `/a[sessionId=1]`.

- Class `DistributedConflictExample` creates a conflict on node `/a` by two sessions originating from two different cluster nodes. The expected behavior is that `session1` succeeds and when `session2` (originating from the second cluster) commits, it needs to wait until the background-read finishes to make the conflict visible. Once the conflict is visible, `session2` aborts with a `CommitFailedException`. The node looks like `/a[sessionId=1]`.

- Class `LocalConflictInIndexExample` sets up a `PropertyIndex` on property `sessionId` and initially creates the nodes `/a/b[sessionId=0]` and `/a/c`. In the experiment property `sessionId=0` is deleted from `/a/b` and added to `/a/c`, provoking a conflict in the index at `/oak:index/sessionId/:index/0/a`. The excpected behavior is that Oak lets one of the two transactions `T1` proceed, while the other `T2` is suspsended until `T1` becomes visible. After `T2` resumes, a rebase operation solves the conflict and also `T2` may commit.

- Class `DistributedConflictInIndexExample` has the exact same setup as the previous class, but changes nodes `/a/b` and `/a/c/` on two different cluster nodes. The excpected behavior is that Oak detects this conflict and suspends `session2` until the conflict becomes visible due to the background-read (as the log output indicates). After the conflict is visible, the `rebase()` and `merge()` are retried and succeed.

- Class `LocalRebaseInIndexExample` is similar to `LocalConflictInIndexExample`, with the difference that `T1` finishes to commit before `T2` starts to commit. Hence a `rebase` in `T2` suffices to resolve the conflict and no error is logged.
