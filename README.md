# circuit-breaker-pattern

面对大流量高并发的分布式系统, 系统互相调用情况复杂,如果依赖的子系统挂了,整个系统都会拖住, 很容易造成系统雪崩
`限流`, `设置超时时间`都不能从根本解决问题, 而熔断模式为了解决这个问题而诞生.

熔断器模式的Python实现


![熔断模式](./state.png)
![熔断模式](./sketch.png)


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


More:
* http://martinfowler.com/bliki/CircuitBreaker.html
* https://www.awsarchitectureblog.com/2015/03/backoff.html
