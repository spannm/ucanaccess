# Contributing to UCanAccess

Thanks for your interest in improving UCanAccess! Contributions of any size are welcome.

## Reporting Bugs

Before opening an issue, please check whether it has already been reported.
A good bug report includes:

- The UCanAccess version and Java version in use
- The Access file format (`.mdb` / `.accdb`) and version (2000, 2003, 2007, ...)
- The JDBC URL / connection parameters used
- A minimal SQL statement or code snippet that reproduces the problem
- The full stack trace
- If possible, a minimal sample database that triggers the issue (with any sensitive data removed)

Reports without a way to reproduce the problem are much harder to act on and may be closed if no reproduction can be provided.

For security vulnerabilities, please follow [SECURITY.md](SECURITY.md) instead of opening a public issue.

## Development Setup

- Java 11 or higher (JDK 17/21 also supported)
- Maven (a wrapper is included: `./mvnw`)

```bash
./mvnw verify
```

This runs the build, unit tests, and static analysis (Checkstyle, PMD, SpotBugs). Please make sure it passes locally before opening a pull request — the same checks run in CI.

Note that UCanAccess builds on [Jackcess](https://github.com/spannm/jackcess) for the underlying Access file access. Bugs related to reading/writing the raw `.mdb`/`.accdb` file format (as opposed to JDBC/SQL behavior) often belong in that repository instead.

## Making Changes

- Keep pull requests focused on a single change; unrelated cleanups make review harder.
- Add or update unit tests (JUnit 5) for any behavioral change.
- Follow the existing code style; Checkstyle/PMD will flag most deviations automatically.
- Write commit messages in [Conventional Commits](https://www.conventionalcommits.org/) style (`fix:`, `feat:`, `docs:`, `build:`, `refactor:`, `test:`, ...), matching the existing commit history.

## Submitting a Pull Request

1. Fork the repository and create a branch from `master`.
2. Make your changes, including tests.
3. Ensure `./mvnw verify` passes.
4. Open a pull request describing the change and, if applicable, the issue it fixes.

CI must pass before a pull request can be merged. Feel free to ask questions in the PR if anything about the process is unclear.

## License

By contributing, you agree that your contributions will be licensed under the Apache License, Version 2.0, the same license that covers this project.
