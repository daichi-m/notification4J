# JNotify

JNotify is a Java-based library to implement a user-targeted notification system for a Microservice-based application. 

It was developed in @WalmartLabs as part of the Machine Learning Platform inside Walmart.

## Design

JNotify exposes a set of Java API that provides the user with the following features:
1. Push a notification for a user to a backend store.
2. Mark a notification as ACKNOWLEDGED, DELETED or EXPIRED
3. Automatically purge expired or deleted notifications from the backend store.

JNotify uses Redis as the backing store for storing the notifications. It can be easily extended to uses any other store as required.

For detailed discussion on the design and implementation, please visit our Medium blog about JNotify.

## Usage

The primary interface that is exposed is `NotifierClient`. JNotify uses JSR-330 annotations for Dependency Injection. So any framework that implements JSR-330 should be able to automatically inject an instance of `NotifierClient` properly.

`NotifierClient` requires an instance of `NotificationConfiguration` to initialize the system properly. `NotificationConfiguration` is a POJO of all configuration params, and it can either be initialized in code, or can be read in from a config file (e.g., a JSON file).


### Getting an instance of NotifierClient

#### Using Spring for DI

In one `@Configuration` class, the following bean can be exposed.
```
@Bean
public NotificationConfiguration createNotificationConfig() {
    return new NotificationConfiguration()
        .setRedisHost("localhost")
        .setRedisPort(redisServer.ports().get(0))
        .setRedisDatabase(0) 
        .setRedisConnectionTimeout(1000)
        .setIdleRedisConnections(3)
        .setMaxRedisConnections(6)
        .setBackOffDelayMillis(1000)
        .setMaxBackOffDelayMillis(MAX_DELAY)
        .setMaxAttempts(MAX_ATTEMPTS)
        .setDefaultExpiry(Duration.ofDays(7))
        .setDefaultSource("TEST")
        .setDefaultSeverity(Notification.Severity.INFO);
}
```

In the `@Component` class the following code would inject a proper instance of `NotifierClient`

```
@Component
public class TestNotification {
    
    // You can also use Spring's @Autowired
    @javax.inject.Inject 
    private NotifierClient client;
    
    ...

}
```

#### Using Guice for DI

The `Module` can bind the `NotificationConfiguration` class to an initialized instance.
```
public class AwesomeModule extends AbstractModule {
    public void configure() {
        // Your awesome bindings
        NotificationConfiguration config = new NotificationConfiguration()
           .setRedisHost("localhost")
           .setRedisPort(redisServer.ports().get(0))
           .setRedisDatabase(0) 
           .setRedisConnectionTimeout(1000)
           .setIdleRedisConnections(3)
           .setMaxRedisConnections(6)
           .setBackOffDelayMillis(1000)
           .setMaxBackOffDelayMillis(MAX_DELAY)
           .setMaxAttempts(MAX_ATTEMPTS)
           .setDefaultExpiry(Duration.ofDays(7))
           .setDefaultSource("TEST")
           .setDefaultSeverity(Notification.Severity.INFO);
        bind(NotificationConfiguration.class).toInstance(config);
    }
}
```

Then any class can inject the proper `NotifierClient` by using a simple `@Inject`.

```
public class AwesomeClass {
    
    // Can also be Guice's @Inject.
    @Inject
    private NotifierClient client;
    ....
}
```

> **_NOTE:_** Guice does not have native support for lifecycle manager (`@PostConstruct`, `@PreDestroy` etc). If you are using Guice, please ensure to use Netflix's Governator on top of it, and enable Governator's LifeCycleManager feature.


### Pushing a notification to the store

In order to push a notification to the store, you can use the `notifyUser` method of the `NotifierClient`. Example code provided below.

```
public void somethingHappened() {
    Notification notfn = new Notification()
       .setMesage("Something terrible happened")
       .setDescription("So long and thanks for all the fish.")
       .setSeverity(Notification.Severity.INFO)
       .setSource("Vogon")
       .setStatus(Notification.Status.NOT_ACKNOWLEDGED)
       .expireAfter(Duration.ofMinutes(10));
   
    // If you want to use the default error handler (which just logs an error message using Slf4J logger).
    client.notifyUser("ford_prefect", notfn);
    
    // If you want to handle the error yourself.
    client.notifyUser(ex -> handleError(ex), "ford_prefect", notfn);
}  
```

### Cleaning up the expired notifications
JNotify integrates with Quartz scheduler that runs a scheduled purge thread every 30minutes that scans the backend store and cleans up all expired and deleted notifications. 
No additional steps are required from the user to enable this. 


## LICENSE

JNotify is licensed under MIT License. Please go through the [LICENSE](LICENSE) file to understand what it covers. 
