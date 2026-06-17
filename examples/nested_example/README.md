# Nested Routing Example

This example demonstrates complex routing hierarchies with FastSQS using subrouters.

## Features

- Nested routing with subrouters
- Built-in middleware usage (Logging, Timing)
- Complex message routing patterns
- Fallback handlers

## Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Run the example
python lambda_function.py
```

## What it demonstrates

- Creating main router with subrouters
- Hierarchical message routing (action -> entity, action -> db)
- Built-in middleware integration
- Default route handling
- Lambda function integration

## Routing Structure

```
action: create -> entity: user/order
action: write -> db: rds/cache
action: update/delete/search -> direct handlers
action: unknown -> default handler
```

## Expected Output

The example will test various routing scenarios and show how nested routing works.
