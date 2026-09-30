# Graph traversal

## Algorithm

Breadth-first search from the origin dataset, following edges in the downstream
direction.

```
visited = { origin: 0 }          # dataset -> shallowest depth reached
queue   = [(origin, 0)]
edges   = []

while queue:
    node, d = queue.popleft()
    if d >= depth_limit:
        continue
    for job in jobs_touching(node):
        for out in job.outputs:
            edges.append((node, job, out))
            if out not in visited or visited[out] > d + 1:
                visited[out] = d + 1      # record the SHALLOWEST depth
                queue.append((out, d + 1))
```

## Why the visited set stores a depth

A boolean visited set is wrong here.

Consider: node `X` is first reached at depth 5 via a long chain, so it is marked visited.
Later, `X` is reachable at depth 2 via a short chain. With a boolean visited set the
second arrival is pruned, so `X`'s descendants are never expanded — even though they sit
at depth 3, well inside a depth limit of 5. **Result: silently missing lineage.**

Storing the shallowest depth fixes this: re-expand only when a strictly shorter path is
found. This also keeps the worst case bounded, because each node can be re-expanded at
most `depth_limit` times.

## Cycle handling

Two kinds of cycles appear:

| Cycle | Example | Verdict |
|---|---|---|
| **Self-loop** | `INSERT INTO t ... SELECT ... FROM t` | **By design.** Incremental datasets read their own prior state. Expected. |
| **Cross-job cycle** | A → B → C → A | **Defect.** Scheduling dependencies are misconfigured. |

Both are handled by the same visited-set mechanism. The difference is *semantic*, not
algorithmic — which is why the traversal cannot classify them on its own. The self-loop
is distinguishable because it appears inside a single job's SQL; a cross-job cycle
requires walking multiple jobs.

## Depth limit

Default is **5**. Rationale:

- Weekly → daily → hourly chains typically resolve within 3–4 hops.
- Beyond 5, node count grows faster than the information gained.
- Keeps a single traversal in the ~10 second range for graphs of 10–100 nodes.

Make it a parameter rather than a constant: blast-radius questions want the default,
full-estate audits want it raised.

## Scope boundary

Traversal follows the data plane and stops where the scheduling system changes. Do not
attempt to bridge control-plane metadata sources with inconsistent schemas without a
canonical model — a correct partial graph is more useful than a confident wrong one.

