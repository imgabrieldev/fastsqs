# Real AWS Lambda Python runtime image (bundles the Runtime Interface Emulator).
# Build context = repo root:  docker build -f tests/integration/Dockerfile -t fastsqs-rie .
FROM public.ecr.aws/lambda/python:3.13

# Install the library's runtime dependencies + the local fastsqs package.
RUN pip install --no-cache-dir "pydantic>=2" "fast-depends>=3"
COPY fastsqs/ ${LAMBDA_TASK_ROOT}/fastsqs/
COPY tests/integration/rie_app.py ${LAMBDA_TASK_ROOT}/

# handler = <module>.<function>
CMD ["rie_app.lambda_handler"]
