# Depends

`Depends` is re-exported from [fast-depends](https://lancetnik.github.io/FastDepends/)
so you can declare dependencies on a handler without an extra import.

```python
from fastsqs import Depends
```

Declare a `Depends(...)` parameter on a handler and fastsqs resolves it per
invocation; sub-dependencies resolve recursively. See
[Inject dependencies](../guide/dependency-injection.md) for usage.

For the full dependency API, see the
[fast-depends documentation](https://lancetnik.github.io/FastDepends/).
