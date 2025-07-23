# Codecrafters Redis Elixir

My take at doing ["build your own redis"]() in elixir through [codecrafters]().

## Brainstorm

how to strucutre the code?
goals:

- adding one part shouldn't feel like i'm breaking other parts of the system
- split based on things making sense instead of convention
- tests?

- parts:
  - handling replication connection
  - handling propagation
  - handling handshake
  - ets parts
  - storage
  - commands
  - utils
