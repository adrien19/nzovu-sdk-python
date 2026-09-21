# Contributing to the Nzovu Python SDK

## Development setup

This independent repository is being migrated. SDK-PR0 keeps the existing
`chronoqueue` package/API intact; package renaming follows in SDK-PR1.
See [the bootstrap baseline](docs/MIGRATION_BASELINE.md) for validation status.

Use Python 3.12 and Poetry 2.3.1 for the bootstrap baseline. Python 3.10–3.14
remain the inherited CI matrix; broader compatibility is validated during migration.

```bash
cd SDKs/nzovu-sdk-python
make install-dev
make test
make lint
make typecheck
make format FORMAT_FLAGS=--check
make build
```

The locked install creates `.venv` and includes development tools and optional
Pydantic models. `make install` installs only the base runtime dependencies.
Run tools through `poetry run` or the Makefile to use this environment.

Alternatively, open this repository folder in VS Code and select **Reopen in
Container**. The container pins Python and Poetry; post-create installs the
committed lockfile. Bootstrap does not require Docker socket access or launch
Redis or other services. Live Nzovu integration setup follows in SDK-PR4.

Work from `main` on a feature branch. Commit and push only after explicit user
approval. Publishing workflows and local publish targets are disabled until the
new package release flow is reviewed in SDK-PR5.

## Development Workflow

### Making Changes

1. Create a new branch for your feature or bugfix:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes following the coding standards below

3. Run tests:
   ```bash
   make test
   ```

4. Run linting and formatting:
   ```bash
   make format
   make lint
   ```

5. Commit your changes with a descriptive message:
   ```bash
   git commit -m "feat: Description of your changes"
   ```

6. Push to your fork and create a pull request

### Coding Standards

- **Code Style**: We use Black for code formatting (120 character line length)
- **Import Sorting**: We use isort with Black-compatible settings
- **Type Hints**: Use type hints where appropriate
- **Documentation**: Add docstrings for public functions and classes
- **Testing**: Write tests for new functionality

### Running Quality Checks

Before submitting a PR, ensure all checks pass:

```bash
# Format code
make format

# Run linting
make lint

# Run type checking
make typecheck

# Run tests with coverage
make test-coverage

# Run all CI checks
make ci
```

### Updating Proto Definitions

The project uses proto definitions from the [chronoqueue repository](https://github.com/adrien19/chronoqueue). To update them:

1. Set your GitHub token (required for private repo access):
```bash
export GITHUB_TOKEN=your_github_token
```

You can create a token at: https://github.com/settings/tokens (needs `repo` scope)

2. Download the latest proto definitions:
```bash
make update-proto
```

3. Regenerate Python classes:
```bash
make gen-proto
```

This will:
- Generate Python gRPC classes from all `.proto` files
- Reorganize files to `chronoqueue/api/` (removing `proto/` prefix)
- Fix imports to use `chronoqueue.api.*` instead of `proto.*`
- Use relative imports within packages
- Format the generated code with Black and isort
- Create proper Python package structure

**Important:** Generated files in `chronoqueue/api/{common,google,message,queue,queueservice,schedule,schema}/` are **checked into version control**. This allows users to install the SDK without needing build tools. After running `make gen-proto`, commit the changes.

**Advanced Configuration:**

You can override the default repository, branch, or proto path:

```bash
# Use a different branch
CHRONOQUEUE_BRANCH=main make update-proto

# Use a fork or different repo
CHRONOQUEUE_REPO=youruser/chronoqueue make update-proto

# Use a different proto directory
CHRONOQUEUE_PROTO_PATH=api/proto make update-proto
```

### Regenerating Proto Files

If you modify proto definitions locally or want to regenerate from scratch:

```bash
make clean-all  # Remove all generated proto code
make gen-proto  # Regenerate with formatting
```

**Note on clean targets:**
- `make clean` - Removes build artifacts and cache (preserves generated proto code)
- `make clean-all` - Removes everything including generated proto code (use before regenerating)

## Testing

### Writing Tests

- Place test files in the `tests/` directory
- Name test files with `test_` prefix (e.g., `test_client.py`)
- Use pytest fixtures and markers appropriately
- Aim for high test coverage

### Running Tests

```bash
# Run all tests
make test

# Run with coverage
make test-coverage

# Run specific test file
poetry run pytest tests/test_client.py -v
```

## Pull Request Process

1. Update the README.md or documentation if needed
2. Ensure all tests pass and coverage is maintained
3. Update the CHANGELOG.md with notable changes
4. Request review from maintainers
5. Address any feedback from code review

### PR Checklist

- [ ] Code follows project style guidelines
- [ ] Tests added/updated and passing
- [ ] Documentation updated if needed
- [ ] CHANGELOG.md updated
- [ ] All CI checks passing
- [ ] Commit messages are clear and descriptive

## Commit Message Guidelines

- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit the first line to 72 characters
- Reference issues and pull requests after the first line

Examples:
```
Add support for async operations

- Implement async client methods
- Add tests for async functionality
- Update documentation

Closes #123
```

## Code of Conduct

- Be respectful and inclusive
- Welcome newcomers and help them get started
- Focus on what is best for the community
- Show empathy towards other community members

## Questions?

If you have questions, feel free to:
- Open an issue for discussion
- Reach out to maintainers
- Check existing documentation

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
