# FastSQS Examples

This folder contains organized examples demonstrating different features of the FastSQS library.

## Examples Overview

### 1. [Simple Standard Example](./simple_standard_example/)
Basic usage with Standard SQS queues. Demonstrates:
- Simple message routing
- Standard queue processing (parallel)
- Basic event handling

### 2. [Simple FIFO Example](./simple_fifo_example/)
Basic usage with FIFO SQS queues. Demonstrates:
- FIFO queue processing (strict ordering)
- Message group and deduplication handling
- Queue type awareness

### 3. [Nested Example](./nested_example/)
Complex routing with subrouters. Demonstrates:
- Nested routing hierarchies
- Built-in middleware usage
- Lambda function integration
- Docker containerization

### 4. [Middleware Example](./middleware_example/)
Built-in middleware usage. Demonstrates:
- LoggingMiddleware with field masking
- TimingMiddleware for performance
- Sensitive data handling

### 5. [Custom Middleware Example](./custom_middleware_example/)
Custom middleware creation. Demonstrates:
- Custom middleware classes
- Error handling middleware
- Metrics collection
- Custom context management

## Getting Started

Each example folder contains:
- `lambda_function.py` - The main example code
- `requirements.txt` - Dependencies
- `README.md` - Detailed explanation

## Running Examples

```bash
# Navigate to any example folder
cd simple_standard_example

# Install dependencies
pip install -r requirements.txt

# Run the example
python lambda_function.py
```

## Docker Support

The nested example includes Docker support for containerized testing:
```bash
cd nested_example
docker-compose up --build
```

## Testing

Use the provided test scripts or run the examples directly to see FastSQS in action.
