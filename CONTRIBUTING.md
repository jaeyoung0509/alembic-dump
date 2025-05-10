# Contributing to Alembic Dump

First off, thank you for considering contributing to `alembic-dump`! Your help is greatly appreciated.

## How Can I Contribute?

There are many ways you can contribute to the project:

* **Reporting Bugs**: If you find a bug, please open an issue on GitHub. Include as much detail as possible: how to reproduce the bug, what you expected, and what actually happened.
* **Suggesting Enhancements**: If you have an idea for a new feature or an improvement to an existing one, open an issue to discuss it.
* **Writing Code**: If you want to fix a bug or implement a feature, feel free to submit a pull request.
* **Improving Documentation**: Clear documentation is crucial. If you find parts of the documentation unclear or missing, please let us know or submit a pull request to improve it.
* **Writing Tests**: Adding more tests helps ensure the reliability of the project.

## Development Setup

1.  Fork the repository on GitHub.
2.  Clone your fork locally:
    ```bash
    # TODO: Replace with your fork's URL
    git clone [https://github.com/YOUR_USERNAME/alembic-dump.git](https://github.com/YOUR_USERNAME/alembic-dump.git)
    cd alembic-dump
    ```
3.  Create a new branch for your changes:
    ```bash
    git checkout -b name-of-your-feature-or-fix
    ```
4.  Set up your development environment (we recommend using UV):
    ```bash
    uv venv .venv --python 3.9 # Or your preferred Python 3.9+ version
    source .venv/bin/activate
    uv pip install ".[dev]"
    ```
5.  Make your changes.
    * Ensure your code follows the existing style (primarily Black and Ruff).
    * Add tests for any new features or bug fixes.
    * Update documentation if necessary.

6.  Run tests to ensure everything still works:
    ```bash
    pytest
    ```
7.  Format and lint your code:
    ```bash
    uv run black .
    uv run ruff check . --fix
    ```
8.  Commit your changes with a clear commit message:
    ```bash
    git commit -m "feat: Add awesome new feature" # Or fix:, docs:, style:, refactor:, test:, chore:
    ```
9.  Push your branch to your fork:
    ```bash
    git push origin name-of-your-feature-or-fix
    ```
10. Open a pull request from your fork to the main `alembic-dump` repository. Provide a clear description of your changes.

## Pull Request Guidelines

* Ensure your PR addresses an open issue or discusses a new feature/fix.
* Keep your PRs focused. Submit separate PRs for unrelated changes.
* Ensure all tests pass.
* Update documentation and changelog if your changes require it.
* Be responsive to feedback and questions during the review process.

## Code of Conduct

This project and everyone participating in it is governed by a [Code of Conduct](CODE_OF_CONDUCT.md) (TODO: Create this file if you wish, or remove this line). By participating, you are expected to uphold this code.

Thank you for your contributions!