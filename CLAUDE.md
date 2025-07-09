# Claude Code Context

This file is automatically loaded when starting a new Claude Code conversation.

## Rules:

1. use `uv run py...` for all python commands so that you use the connrect dependencies.
2. Tests should use pytest fixtures, not classes. Use Arrange Act Assert organization for tests.
3. Test logic should not exist in production code. Any file not starting with `test_...` cannot have test logic.
4. Tests should not have conditional logic. Ideally everythihing is == operator.

## Current Project

See memory bank: `proj_7/PROJECT_INDEX.md`
