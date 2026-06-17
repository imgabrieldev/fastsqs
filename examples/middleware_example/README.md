# Built-in Middleware Example

This example demonstrates how to use FastSQS built-in middleware.

## Features

- LoggingMiddleware with field masking
- TimingMiddleware for performance measurement
- Sensitive data handling
- Built-in middleware integration

## Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Run the example
python lambda_function.py
```

## What it demonstrates

- Adding built-in middleware to FastSQS
- Field masking for sensitive data (password, SSN)
- Performance timing measurement
- Middleware execution order

## Middleware Used

- **LoggingMiddleware**: Masks sensitive fields and logs messages
- **TimingMiddleware**: Measures processing time in milliseconds

## Expected Output

The example will process messages with sensitive data and show how middleware handles logging and timing.
