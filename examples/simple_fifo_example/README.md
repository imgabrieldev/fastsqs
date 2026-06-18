# Simple FIFO Queue Example

This example demonstrates basic usage of FastSQS with FIFO SQS queues.

## Features

- FIFO queue processing (strict ordering)
- Message group and deduplication handling
- Queue type awareness in handlers

## Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Run the example
python lambda_function.py
```

## What it demonstrates

- Creating a QueueApp with explicit FIFO queue type
- Handling FIFO-specific attributes (MessageGroupId, MessageDeduplicationId)
- Queue type detection in message handlers
- Sequential processing within message groups

## Expected Output

The example will process three FIFO messages and show how FIFO queues maintain order within message groups.
