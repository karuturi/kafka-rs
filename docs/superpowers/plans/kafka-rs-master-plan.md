**Strict Mandate:** Use Test-Driven Development (TDD) for every feature and bug fix. Make a commit after every successful step or logical unit of work.

# Kafka-RS Master Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a Modern Kafka 3.0+ compatible broker in Rust for learning purposes.

---

## Phase 1: Foundation (Single Broker & Storage) ✅
*Focus: Networking, Storage Engine, and Basic CLI Compatibility.*
- [Plan: kafka-rs-phase1.md] ✅

## Phase 2: Protocol Deep-Dive (The "Translator")
*Focus: Custom binary protocol implementation (Replacing the crate).*
- [Plan: kafka-rs-phase2.md]

## Phase 3: Consumer Groups (The "Manager")
*Focus: Group Coordination, JoinGroup/SyncGroup, and Rebalancing.*
- [Plan: kafka-rs-phase3.md]

## Phase 4: Distributed Brain (The "Brain")
*Focus: KRaft Consensus, Leader Election, and Partition Replication.*
- [Plan: kafka-rs-phase4.md]
