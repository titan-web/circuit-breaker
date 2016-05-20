# circuit-breaker-pattern
熔断器模式的Python实现

*Example*:

```
fuses_manage = FusesManager()
f = fuses_manage.get_fuses('push', 0, 1, [RuntimeError])
try:
    with circuit(f) as a:
        # remote call raise error
        raise RuntimeError("self runtime error!")

except FusesOpenError as exp:
    # do what you want when error
    print exp
```

![熔断模式](./circuit_breaker_pattern.jpg)
