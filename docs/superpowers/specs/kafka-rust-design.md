# Design Spec: Kafka-RS (Modern Rust Broker Rewrite - Phase 1)

**Date:** 2026-04-02  
**Topic:** Core Storage Engine, Networking, and CLI Compatibility  
**Author:** Gemini CLI & User  

---

## 1. Objective
A 100% Safe Rust implementation of a Kafka 3.0+ compatible broker. The goal is to learn Kafka's internals by building the storage engine, protocol handling, and coordination from the ground up, starting with a single-broker system that works with existing Kafka CLIs (`kafka-console-producer`, `kafka-console-consumer`).

## 2. Architecture Overview

### 2.1 Networking & Protocol
- **Runtime:** `tokio` (Async I/O).
- **Listener:** TCP on Port 9092.
- **Protocol Handling:** 
  - **Phase 1:** Use `kafka-protocol` crate for robust v3.0+ request/response structures.
  - **Phase 2:** Replace with custom binary parser/encoder for deeper learning.
- **Connection Model:** Each connection is a separate `tokio` task.

### 2.2 Concurrency: The Partition Actor
To handle concurrent producers and consumers without complex locking, each partition (e.g., `orders-0`) is managed by a dedicated **Partition Actor** (a `tokio` task).

- **Communication:** Bounded `mpsc` channel (Capacity: 100).
- **Backpressure:** If the channel is full (disk/IO slow), the connection handler stops reading from the socket until space is available.
- **Responsibility:** The Actor is the **exclusive owner** of its log and index files.

---

## 3. Storage Engine: Segmented Logs & Sparse Index

### 3.1 Directory Structure
We follow the standard Kafka layout for CLI compatibility:
```text
/storage/
  /orders-0/
    0000000000.log      # Active segment (messages starting at offset 0)
    0000000000.index    # Sparse index for the segment
  /orders-1/
    ...
```

### 3.2 Segment Management
- **Active Segment:** The only file currently being appended to.
- **Rotation:** When the `.log` reaches a threshold (e.g., 10MB for testing), the Actor:
  1. Closes the current segment and index.
  2. Creates a new `.log` file named after the next starting offset.
- **Retention:** Basic time-based or size-based cleanup (Phase 1.5).

### 3.3 Sparse Indexing
- **Mechanism:** Add an entry to the `.index` file every **4,096 bytes** of appended data.
- **Index Format:** Binary mapping of `RelativeOffset -> PhysicalPosition`.
- **Search Algorithm:** 
  1. Binary search the in-memory sparse index for the largest offset <= target.
  2. `seek()` to that physical position in the `.log`.
  3. Scan linearly until the target offset is found.

---

## 4. Metadata & CLI Compatibility

### 4.1 Single-Broker Mode (KRaft-Lite)
- **Controller:** The broker acts as its own controller (Node ID: 1).
- **Registry:** A simple in-memory `HashMap` of topic names to partition actors.
- **Storage:** Metadata is persisted to a local `metadata.toml` or internal `@metadata` log.

### 4.2 Minimal API Implementation (v3.0+)
To support `kafka-console-producer/consumer`, we must implement:
1. `ApiVersions` (v3) - Negotiate protocol versions.
2. `Metadata` (v12) - Report topic/partition locations.
3. `Produce` (v9) - Accept message batches.
4. `Fetch` (v12) - Stream messages to consumers.
5. `ListOffsets` (v7) - Find beginning/end of log.

---

## 5. Implementation Roadmap (Phase 1)

1. **Scaffold:** `tokio` server and TCP handshake logic.
2. **Metadata Layer:** Respond to `ApiVersions` and `Metadata` requests.
3. **Storage Engine:** Implement the `PartitionActor` with basic `.log` appending.
4. **Indexing:** Add the Sparse Index logic for fast lookups.
5. **Consumer Support:** Implement `Fetch` and `ListOffsets`.
6. **Validation:** Connect via `kafka-console-producer` and verify data persistence.

---

## 6. Success Criteria
- [ ] Successfully start a `tokio` listener on 9092.
- [ ] `kafka-topics --bootstrap-server localhost:9092 --list` returns a topic.
- [ ] `kafka-console-producer` can send messages without error.
- [ ] Messages are visible as binary data in the `.log` file.
- [ ] `kafka-console-consumer` can retrieve messages starting from offset 0.
