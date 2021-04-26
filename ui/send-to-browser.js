app.use("/getNotifications", (req, res) => {
  // check auth for current user
  let loggedUser = getLoggedinUser(req);

  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
  });
  res.setTimeout(0);
  res.write("\n");
  readMessage(`${redisKeyPrefix}${loggedUser}`); //read user’s message just after getting connected to server

  function reqNotificationLister(key, value) {
    if (`${redisKeyPrefix}${loggedUser}` === key) {
      const userNotification = `{"type":"user","notifications":${value}}`;

      sendNotification(req, res, userNotification);
    }
  }

  eventEmitter.on("notification", reqNotificationLister);
  req.on("finish", () => {
    // cleanup resources
  });
  req.on("close", () => {
    eventEmitter.removeListener("notification", reqNotificationLister);
  });
});

const sendNotification = (req, res, notifications) => {
  const id = Date.now();
  res.write(`id: ${id}\n`);
  res.write(`data: ${notifications} \n\n`);
  res.flush();
};
