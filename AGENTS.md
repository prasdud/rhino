# AGENTS.md — Agent Guidelines for Rhino Project

This file contains instructions and guidelines for AI agents working on the Rhino job queue project.

## MCP Integration

### Excalidraw MCP Server

The project has the Excalidraw MCP server configured for creating diagrams and visualizations.

**When to use Excalidraw:**
- Create architecture diagrams showing system components and their relationships
- Visualize the worker loop flow and job processing pipeline
- Generate sequence diagrams for API interactions
- Document data flow between components (queue, worker, database)
- Create component diagrams for library structure (lib.rs, worker.rs, etc.)
- Illustrate retry logic and backoff mechanisms
- Show state transitions for job statuses (pending → locked → done/failed/dead)

**How to invoke Excalidraw:**
Add `use excalidraw` to your prompts when you want to create diagrams. For example:
- "Create an architecture diagram showing how the worker processes jobs use excalidraw"
- "Draw a sequence diagram showing the job enqueue and execute flow use excalidraw"

**Available Excalidraw tools:**
- `read_me` - Get reference for Excalidraw element format with examples
- `create_view` - Render hand-drawn diagrams with streaming animations

## Project Context

Rhino is a durable, exactly-once job queue built in Rust with Postgres as the only dependency.

**Key Components:**
- `src/lib.rs` - Public API surface
- `src/worker.rs` - Worker loop, polling, locking, execution
- `src/main.rs` - CLI entry point
- Database: `rhino_jobs` table with job status tracking
- Core guarantee: `SELECT FOR UPDATE SKIP LOCKED` for exactly-once execution

**Job Status Flow:**
```
pending → locked → done
                 → failed (retry with backoff)
                 → dead (max attempts exceeded)
```

## Coding Standards

- No `.unwrap()` in library code - use proper error handling
- All public functions must have doc comments
- Follow the existing code style and conventions
- Use `thiserror` for error types
- Prefer `Arc` for shared state over `Mutex` when possible
- Minimize allocations in hot paths (especially worker loop)

## Testing Priority

The exactly-once guarantee is sacred. Any change must maintain zero duplicates under concurrent worker scenarios. The stress test in `stress/stress_test.rs` must always pass.