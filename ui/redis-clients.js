let readClient = null;
let subClient = null;

const redisChannel = "KEYS_CHANGED";
const redisConnectionUrl = `redis://${redis_host}`;
function connectToRedis() {
  readClient = redis.createClient(redisConnectionUrl);
  subClient = redis.createClient(redisConnectionUrl);
  readClient.on("error", function (err) {
    // handle error
  });

  subClient.on("error", function (err) {
    // handle error
  });

  subClient.on("message", (channel, message) => {
    const changedKeys = JSON.parse(message);
    Array.isArray(changedKeys) &&
      changedKeys.forEach((key) => readMessage(key));
    subClient.subscribe(redisChannel);
  });
}

function readMessage(key) {
  readClient.hgetall(key, function (err, value) {
    if (err) {
      // handle failure
    }
    eventEmitter.emit("notification", key, JSON.stringify(value));
  });
}
