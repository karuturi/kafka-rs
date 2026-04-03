# Gemini CLI Mandates

## Development Workflow
- **Strict TDD:** NEVER write production code without a failing test first. Watch it fail, then write minimal code to pass.
- **Frequent Commits:** Commit after every successful step or logical unit of work. Do not wait for a full feature to be complete.
- **Commit Messages:**
    - Focus on **WHY**, not just what.
    - Avoid generic prefixes like `chore:`. Use `feat:`, `fix:`, `refactor:`, `test:`, or `docs:` with descriptive context.
    - Provide enough detail in the body to explain the architectural or technical rationale.

## Technical Standards
- **Async First:** Use `tokio` for all asynchronous operations.
- **Safety:** Leverage Rust's type system and `anyhow` for robust error handling.
- **Kafka Protocol:** Maintain compatibility with Kafka 3.0+ protocol specs.
