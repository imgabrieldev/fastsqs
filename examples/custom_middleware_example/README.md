# Custom Middleware Example

This example demonstrates how to create and use custom middleware with FastSQS.

## Features

- Custom middleware classes
- Error handling middleware
- Metrics collection middleware
- Custom logging middleware

## Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Run the example
python lambda_function.py
```

## What it demonstrates

- Creating custom middleware classes
- Implementing before/after hooks (handle failures in `after` via the `error` parameter)
- Error handling and recovery
- Metrics collection and reporting
- Custom context management

## Custom Middleware Classes

- **CustomLoggingMiddleware**: Detailed message processing logs
- **ErrorHandlingMiddleware**: Graceful error handling
- **MetricsMiddleware**: Performance metrics collection

## Expected Output

The example will process messages and demonstrate custom middleware functionality including error handling and metrics collection.
