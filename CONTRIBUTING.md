# Contributing to UltraCache

Thanks for your interest in UltraCache! This guide explains how to report issues, propose enhancements, and contribute code.

---

## 📌 Before You Start

Please read:
- [README.md](README.md)
- [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md)
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

---

## 🐛 Reporting Issues

When filing a bug, include:

- UltraCache version / commit hash
- OS and environment
- Steps to reproduce (minimal)
- Expected vs actual behavior
- Logs or screenshots (if applicable)

Submit issues via GitHub: **Issues → New Issue**

---

## ✨ Proposing Enhancements

Enhancement requests should include:

- Clear description of the feature
- Use case / motivation
- Expected behavior
- Any implementation ideas or references

Check existing issues before creating a new one.

---

## 🛠 Development Setup

### Requirements
- Rust 1.83+ (2021 edition)
- cargo
- Python 3.x (for integration tests)

### Build
```bash
cargo build --release
```

### Run
```bash
./target/release/ultracache
```

### Tests
```bash
# Rust unit tests
cargo test --release

# Python integration tests
for test in tests/test_*.py; do python3 "$test"; done
```

---

## ✅ Pull Request Guidelines

Before opening a PR:

- [ ] Code builds: `cargo build --release`
- [ ] Tests pass (Rust + Python)
- [ ] No unrelated changes
- [ ] Clear commit message
- [ ] Update docs if behavior changes

PRs should include:
- Summary of changes
- Motivation / issue link
- Any performance or compatibility impact

---

## 📐 Code Style

- Follow Rust `clippy` best practices
- Prefer explicit types when clarity helps
- Keep functions small and focused
- Avoid unnecessary allocations

---

## 🧪 Adding Tests

If you change behavior or add features, include tests:

- Unit tests for core logic
- Integration tests for command behavior

Test files live in `tests/` and use simple RESP clients.

---

## 🤝 Contributor Agreement

By submitting a PR, you agree your contributions are licensed under the Apache 2.0 license.

---

## 📮 Questions

Open a GitHub issue or discussion if you need help.
