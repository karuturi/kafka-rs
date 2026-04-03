**Strict Mandate:** Use Test-Driven Development (TDD) for every feature and bug fix. Make a commit after every successful step or logical unit of work.

# Kafka-RS Implementation Plan - Phase 1

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a single-broker Kafka 3.0+ compatible server with segmented logs and sparse indexing.

**Architecture:** A `tokio`-based async server where each partition is managed by a dedicated Actor. Metadata is managed in-memory, and the storage engine implements sequential appends with sparse indexing.

**Tech Stack:** Rust, `tokio`, `kafka-protocol`, `bytes`.

---

### Task 1: Project Scaffold & TCP Listener ✅

**Files:**
- Create: `Cargo.toml` ✅
- Create: `src/main.rs` ✅
- Create: `src/protocol.rs` ✅

- [x] **Step 1: Initialize Cargo.toml with dependencies** ✅
- [x] **Step 2: Create a basic TCP listener in `src/main.rs`** ✅
- [x] **Step 3: Run the server and verify connectivity** ✅
- [x] **Step 4: Commit** ✅

---

### Task 2: Kafka Wire Framing & ApiVersions ✅

**Files:**
- Modify: `src/main.rs` ✅
- Create: `src/protocol.rs` ✅

- [x] **Step 1: Implement Kafka Frame Reading in `src/main.rs`** ✅
- [x] **Step 2: Implement ApiVersions (v3) Response** ✅
- [x] **Step 3: Test with Kafka CLI** ✅
- [x] **Step 4: Commit** ✅

---

### Task 3: The Partition Actor & Log Appender ✅

**Files:**
- Create: `src/partition.rs` ✅
- Create: `src/storage/mod.rs` ✅
- Create: `src/storage/local.rs` ✅

- [x] **Step 1: Define the Partition Actor message enum** ✅
- [x] **Step 2: Implement the Actor Loop in `src/partition.rs`** ✅
- [x] **Step 3: Implement Basic Append** ✅
- [x] **Step 4: Test with a mock producer** ✅
- [x] **Step 5: Commit** ✅

---

### Task 4: Segment Rotation & Sparse Indexing ✅

**Files:**
- Modify: `src/partition.rs` ✅
- Create: `src/storage/index.rs` ✅

- [x] **Step 1: Implement Segment Rotation** ✅
- [x] **Step 2: Implement Sparse Index** ✅
- [x] **Step 3: Test Rotation** ✅
- [x] **Step 4: Commit** ✅

---

### Task 5: Metadata & Consumer Support (Fetch) ✅

**Files:**
- Modify: `src/main.rs` ✅
- Modify: `src/broker.rs` ✅

- [x] **Step 1: Implement the Broker Registry** ✅
- [x] **Step 2: Implement Metadata Response** ✅
- [x] **Step 3: Implement Fetch API** ✅
- [x] **Step 4: Verify with `kafka-console-consumer`** ✅
- [x] **Step 5: Final Commit & Cleanup** ✅
