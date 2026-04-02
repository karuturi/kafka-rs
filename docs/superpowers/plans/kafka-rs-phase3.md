# Kafka-RS Implementation Plan - Phase 3 (Consumer Groups)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement the Consumer Group Coordinator for stateful consumption.

---

### Task 3.1: Group Coordinator Actor
- [ ] **Step 1:** Implement the `GroupCoordinator` Actor.
- [ ] **Step 2:** Implement `JoinGroup` and `SyncGroup` state machines.
- [ ] **Step 3:** Commit.

### Task 3.2: Rebalancing Logic
- [ ] **Step 1:** Implement the "Range" and "Round Robin" partition assignment strategies.
- [ ] **Step 2:** Integration test: Multiple consumers joining and sharing partitions.
- [ ] **Step 3:** Commit.
