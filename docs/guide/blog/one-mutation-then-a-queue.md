---
title: One mutation, then a queue.
description: A source-guided tour of Temporal History’s update path in v1.31.2.
sidebar: false
---

# One mutation, then a queue.

A workflow change in Temporal is more than a new history event. It can change the execution’s mutable state, create follow-on work, and make that work available to a History queue. The interesting part is that these pieces meet in a single persistence request before a processor is nudged.

This is a reading of Temporal Server **v1.31.2**, pinned to commit [`19a774302c613da9adc4436ab14278ccdca8e0a5`](https://github.com/temporalio/temporal/tree/19a774302c613da9adc4436ab14278ccdca8e0a5). It follows the normal `UpdateWorkflowExecution` persistence path, not every frontend API or every task category.

## The path at a glance

<TemporalMutationDemo />

The queue is not a side effect invented after persistence. The mutable-state close builds its task collection first; the persistence request carries it with the history and state mutation. A later in-process notification helps a queue processor notice new work promptly.

## 1. Close the in-memory transaction

History code works on a mutable state object while it applies a workflow action. When it is ready to persist, [`CloseTransactionAsMutation`](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/service/history/workflow/mutable_state_impl.go#L6914-L6957) calls the shared close routine and constructs a `WorkflowMutation`.

That mutation includes the execution info and state, the next event ID, upsert/delete maps for pending entities, buffered events, and—at [lines 6945–6950](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/service/history/workflow/mutable_state_impl.go#L6945-L6950)—both `Tasks` and the optimistic persistence conditions. Those conditions include the next event ID held in the database and the database record version. They are part of how the write detects a state that changed underneath it.

The close routine deliberately prepares event batches before task preparation. It finishes the history builder, assigns transaction IDs for batches, and updates the execution’s last-first-event metadata in [`closeTransactionPrepareEvents`](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/service/history/workflow/mutable_state_impl.go#L7792-L7859). Then [`closeTransactionPrepareTasks`](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/service/history/workflow/mutable_state_impl.go#L7559-L7589) asks generators for state-machine, timer, retention, and replication-related work. Which task types appear depends on the workflow change and configuration; this is why the article does not promise that every update creates every kind of task.

## 2. Hand persistence a complete request

The workflow transaction packages the mutation and event sequences into `UpdateWorkflowExecutionRequest` in [`TransactionImpl.UpdateWorkflowExecution`](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/service/history/workflow/transaction_impl.go#L163-L216). The shard context assigns task keys and records close-task IDs under its write lock, then sends the request to the execution manager in [`ContextImpl.UpdateWorkflowExecution`](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/service/history/shard/context_impl.go#L628-L687).

The persistence manager serializes the event batches and the workflow mutation, then calls its configured persistence implementation in [`executionManagerImpl.UpdateWorkflowExecution`](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/execution_manager.go#L132-L238). This is the adapter boundary that matters to Xenon: the adapter must accept the complete operation with its conditions, state delta, history batches, and category-indexed tasks.

Temporal’s SQL implementation is useful evidence about ordering, not a template to copy blindly. Its `UpdateWorkflowExecution` appends history nodes first, then runs the mutable-state update under a shard-locked SQL transaction ([lines 334–357](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/sql/execution.go#L334-L357)). Xenon does not claim to be that SQL store. Xenon’s design has to preserve the interface’s observable conditions and outcomes using its own S3-backed transaction and replay rules; the full compatibility proof remains open.

## 3. Persisted tasks first; notification second

After the persistence call returns, the transaction code calls `NotifyOnExecutionMutation` when `OperationPossiblySucceeded(err)` is true ([lines 197–200](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/service/history/workflow/transaction_impl.go#L197-L200)). That helper passes the mutation’s task map to the History engine at [lines 601–617](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/service/history/workflow/transaction_impl.go#L601-L617).

The engine selects a processor by task category and calls `NotifyNewTasks`; replication is handled separately ([lines 882–908](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/service/history/history_engine.go#L882-L908)). For an immediate queue, receiving a nonempty list only signals its loop—it does not make the notification itself the durable queue record ([lines 117–153](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/service/history/queues/queue_immediate.go#L117-L153)).

That distinction is practical after a restart or a dropped notification. The reader builds work from its persisted range. When woken, it selects tasks from a slice and submits each one ([`loadAndSubmitTasks`, lines 426–484](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/service/history/queues/reader.go#L426-L484)). A notification can reduce latency; the stored task is what makes recovery possible.

## 4. A task becomes another operation

Queue submission creates an executable. Its `Execute` method delegates to the category-specific executor and returns that execution error ([lines 229–357](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/service/history/queues/executable.go#L229-L357)). An executor commonly reloads mutable state, validates whether the task is still current, changes state, and persists another workflow update. The loop is intentional: durable state changes produce durable work, and durable work can lead to later state changes.

It is also why a persistence adapter cannot treat History tasks as an optional appendage. Dropping a category’s task record while accepting the associated state update can leave Temporal with state that says work should happen and no durable way to discover it.

## What this means for Xenon—and what it does not

Xenon adapts Temporal’s persistence interfaces; it does not replace History’s mutable state, task generators, or queue executors. For this path, the Xenon-specific obligation is to route the complete persistence operation to the owning storage partition and preserve its conditions, typed response, task data, and replay behavior across a lost reply or node movement.

This source tour does **not** demonstrate that Xenon meets that obligation. It is not a Temporal compatibility result, a measurement, or a substitute for the repository’s reproducible ministack and fault gates. Those boundaries remain in the [verification status](../docs/status.md) and [testing guide](../docs/testing.md).
