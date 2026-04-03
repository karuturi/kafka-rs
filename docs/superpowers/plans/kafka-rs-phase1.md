**Strict Mandate:** Use Test-Driven Development (TDD) for every feature and bug fix. Make a commit after every successful step or logical unit of work.

# Kafka-RS Implementation Plan - Phase 1

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a single-broker Kafka 3.0+ compatible server with segmented logs and sparse indexing.

**Architecture:** A `tokio`-based async server where each partition is managed by a dedicated Actor. Metadata is managed in-memory, and the storage engine implements sequential appends with sparse indexing.

**Tech Stack:** Rust, `tokio`, `kafka-protocol`, `bytes`.

---

### Task 1: Project Scaffold & TCP Listener

**Files:**
- Create: `Cargo.toml`
- Create: `src/main.rs`
- Create: `src/protocol.rs`

- [ ] **Step 1: Initialize Cargo.toml with dependencies**
```toml
[package]
name = "kafka-rust"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = { version = "1.0", features = ["full"] }
kafka-protocol = "0.12.0"
bytes = "1.0"
tracing = "0.1"
tracing-subscriber = "0.3"
anyhow = "1.0"
```

- [ ] **Step 2: Create a basic TCP listener in `src/main.rs`**
```rust
use tokio::net::TcpListener;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092").await?;
    println!("Kafka-RS listening on 127.0.0.1:9092");

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket).await {
                eprintln!("Connection error: {}", e);
            }
        });
    }
}

async fn handle_connection(mut socket: tokio::net::TcpStream) -> Result<()> {
    // Phase 1.1: Just log the connection
    Ok(())
}
```

- [ ] **Step 3: Run the server and verify connectivity**
Run: `cargo run` in one terminal.
Run: `nc -zv 127.0.0.1 9092` in another.
Expected: `Connection to 127.0.0.1 port 9092 [tcp/*] succeeded!`

- [ ] **Step 4: Commit**
```bash
git add .
git commit -m "chore: initial project scaffold and tcp listener"
```

---

### Task 2: Kafka Wire Framing & ApiVersions

**Files:**
- Modify: `src/main.rs`
- Create: `src/protocol.rs`

- [ ] **Step 1: Implement Kafka Frame Reading in `src/main.rs`**
Kafka requests are prefixed by a 4-byte big-endian length.
```rust
async fn handle_connection(mut socket: tokio::net::TcpStream) -> Result<()> {
    use tokio::io::AsyncReadExt;
    let mut len_buf = [0u8; 4];
    socket.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    println!("Received request of length: {}", len);
    Ok(())
}
```

- [ ] **Step 2: Implement ApiVersions (v3) Response**
Use `kafka-protocol` to handle the initial handshake.
```rust
// src/protocol.rs
use kafka_protocol::messages::ApiVersionsRequest;
use kafka_protocol::messages::ApiVersionsResponse;
// (Implementation of encode/decode using kafka-protocol)
```

- [ ] **Step 3: Test with Kafka CLI**
Run: `kafka-topics --bootstrap-server localhost:9092 --list`
Expected: The CLI should receive the `ApiVersions` response and proceed (though it will fail later on Metadata).

- [ ] **Step 4: Commit**
```bash
git add src/
git commit -m "feat: implement kafka framing and apiversions handshake"
```

---

### Task 3: The Partition Actor & Log Appender

**Files:**
- Create: `src/partition.rs`
- Create: `src/storage/mod.rs`
- Create: `src/storage/local.rs`

- [ ] **Step 1: Define the Partition Actor message enum**
```rust
pub enum PartitionCommand {
    Append {
        records: bytes::Bytes,
        resp_tx: tokio::sync::oneshot::Sender<u64>, // returns base offset
    },
    Fetch {
        offset: u64,
        max_bytes: u32,
        resp_tx: tokio::sync::oneshot::Sender<bytes::Bytes>,
    },
}
```

- [ ] **Step 2: Implement the Actor Loop in `src/partition.rs`**
The actor should open a `.log` file in `/storage/<topic>-<partition>/`.

- [ ] **Step 3: Implement Basic Append**
Write bytes directly to the end of the file and return the current offset.

- [ ] **Step 4: Test with a mock producer**
Write a small Rust test that sends `Append` commands to the actor and verifies the file exists on disk.

- [ ] **Step 5: Commit**
```bash
git add src/partition.rs src/storage/
git commit -m "feat: implement partition actor and basic log appender"
```

---

### Task 4: Segment Rotation & Sparse Indexing

**Files:**
- Modify: `src/partition.rs`
- Create: `src/storage/index.rs`

- [ ] **Step 1: Implement Segment Rotation**
If `active_segment.size() > 10MB`, close and open a new file `<offset>.log`.

- [ ] **Step 2: Implement Sparse Index**
Every 4,096 bytes, write `(RelativeOffset, PhysicalPosition)` to `<offset>.index`.

- [ ] **Step 3: Test Rotation**
Send 11MB of data and verify two `.log` files are created.

- [ ] **Step 4: Commit**
```bash
git add src/
git commit -m "feat: implement segment rotation and sparse indexing"
```

---

### Task 5: Metadata & Consumer Support (Fetch)

**Files:**
- Modify: `src/main.rs`
- Modify: `src/broker.rs`

- [ ] **Step 1: Implement the Broker Registry**
A simple `HashMap<String, Vec<mpsc::Sender<PartitionCommand>>>`.

- [ ] **Step 2: Implement Metadata Response**
Always report Node ID 1 as the leader.

- [ ] **Step 3: Implement Fetch API**
Map `FetchRequest` to the `PartitionActor::Fetch` command. Use the index to seek.

- [ ] **Step 4: Verify with `kafka-console-consumer`**
Run: `kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning`

- [ ] **Step 5: Final Commit & Cleanup**
```bash
git add .
git commit -m "feat: complete phase 1 with metadata and fetch support"
```
