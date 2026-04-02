# Kafka-RS Implementation Plan - Phase 2 (Protocol Deep-Dive)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remove the `kafka-protocol` crate and implement the binary logic ourselves.

---

### Task 2.1: Custom Binary Encoders (TDD)
- [ ] **Step 1:** Write failing tests for unsigned varint encoding/decoding.
- [ ] **Step 2:** Implement `Encoder`/`Decoder` traits for `u32` (varint).
- [ ] **Step 3:** Implement CompactString (length-prefixed strings).
- [ ] **Step 4:** Commit.

### Task 2.2: Custom Produce Request Parser
- [ ] **Step 1:** Write unit test comparing our parser's output with a known valid Kafka Produce hex dump.
- [ ] **Step 2:** Implement the parser using the codecs from Task 2.1.
- [ ] **Step 3:** Commit.
