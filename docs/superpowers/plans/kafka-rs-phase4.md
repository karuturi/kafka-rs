**Strict Mandate:** Use Test-Driven Development (TDD) for every feature and bug fix. Make a commit after every successful step or logical unit of work.

# Kafka-RS Implementation Plan - Phase 4 (Distributed Cluster)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement KRaft consensus and Partition Replication.

---

### Task 4.1: The Metadata Log (@metadata)
- [ ] **Step 1:** Implement the system topic for metadata persistence.
- [ ] **Step 2:** Commit.

### Task 4.2: Mini-Raft Consensus
- [ ] **Step 1:** Implement Leader Election and Heartbeats.
- [ ] **Step 2:** Commit.

### Task 4.3: Partition Replication
- [ ] **Step 1:** Implement "Replica Fetcher" and "High Watermark" logic.
- [ ] **Step 2:** Integration test: Failover validation.
- [ ] **Step 3:** Commit.
