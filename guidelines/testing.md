# Testing Guidelines

## Test Best Practices: require vs assert

When writing tests with `testify`, follow these guidelines for choosing between `require` and `assert`:

### Use `require` When:

**Test Setup and Teardown**
- Creating temp files, directories, or test fixtures
- Initializing test objects or dependencies
- Operations that must succeed for the test to run

```go
// Setup - must succeed
f, err := os.CreateTemp("", "test")
require.NoError(t, err)
defer os.Remove(f.Name())

s, err := newStore(f)
require.NoError(t, err)
```

**Critical Operations**
- Code that subsequent test logic depends on
- When a failure means the test cannot meaningfully continue
- Cleanup operations that must complete

```go
// Reopening a file - critical for continuation
s, err = newStore(f)
require.NoError(t, err)

// Teardown
err = log.Close()
require.NoError(t, err)
```

**Error Type Checking**
- Use `require.ErrorIs` for specific error type checks
- More idiomatic than `require.Error` + `require.True(os.IsX(err))`

```go
// Before (verbose)
_, err = os.Stat(filename)
require.Error(t, err)
require.True(t, os.IsNotExist(err))

// After (preferred)
require.ErrorIs(t, err, os.ErrNotExist)
```

### Use `assert` When:

**Validations Inside Loops**
- Allows all iterations to run and report all failures
- Prevents the "fix one, see the next" iteration cycle

```go
for i := 0; i < 100; i++ {
    got, err := log.Read(idx)
    assert.NoError(t, err)        // See all 100 errors
    assert.Equal(t, want, got)    // See all 100 mismatches
}
```

**Multiple Related Field Checks**
- Validating multiple fields of the same object
- When seeing all validation failures is more valuable

```go
// Multiple field validations - see all failures
assert.Equal(t, c.segment.maxIndexBytes, uint64(10*units.MiB))
assert.Equal(t, c.segment.maxStoreBytes, uint64(100*units.MiB))
```

**Non-Critical Checks**
- Validations where the test can continue to provide useful information
- Secondary checks that provide additional context

### Key Benefits

**Before Refactoring (require in loops):**
- Test stops at first failure
- Multiple test runs needed to see all failures
- Example: 100 iterations × 5 checks = 500 potential failure points, but you only see the first one

**After Refactoring (assert in loops):**
- All failures reported in one test run
- Complete diagnostic information immediately
- All 500 potential failures shown in a single run

### Decision Flow

```
Is this setup/teardown or critical for test continuation?
├─ Yes → Use require
└─ No
    ├─ Inside a loop? → Use assert
    ├─ Multiple related checks? → Use assert
    └─ Non-critical validation? → Use assert
```
