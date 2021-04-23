if (!!window.SharedWorker) {
  window.myWorker = new SharedWorker("/getWorker");
  window.myWorker.port.start();
  window.myWorker.port.postMessage(`{"action":"createconnection"}`);
  window.myWorker.port.onmessage = (e) => {
    if (e.data) {
      try {
        this.props.actions.setNotifications(JSON.parse(e.data));
      } catch (error) {
        console.error(error);
      }
    }
  };

  window.addEventListener("beforeunload", () => {
    window.myWorker.port.postMessage(`{"action":"windowclosed"}`);
  });

  heartBeatTimer = setInterval(() => {
    window.myWorker.port.postMessage(
      `{"action":"heartbeat", "activeSince": ${Date.now()}}`
    );
  }, 5000);

  let hidden, visibilityChange;

  if (typeof document.hidden !== "undefined") {
    // Opera 12.10 and Firefox 18 and later support

    hidden = "hidden";
    visibilityChange = "visibilitychange";
  } else if (typeof document.msHidden !== "undefined") {
    hidden = "msHidden";
    visibilityChange = "msvisibilitychange";
  } else if (typeof document.webkitHidden !== "undefined") {
    hidden = "webkitHidden";
    visibilityChange = "webkitvisibilitychange";
  }

  function handleVisibilityChange() {
    if (!document[hidden]) {
      window.myWorker.port.postMessage(`{"action":"readmessage"}`);
    }
  }
  document.addEventListener(visibilityChange, handleVisibilityChange, false);
} else {
  const source = new EventSource("/getNotifications");

  source.addEventListener(
    "message",
    (e) => {
      if (e.data) {
        try {
          const response = JSON.parse(e.data);

          this.props.actions.setNotifications(response.notifications || []);
        } catch (error) {
          console.error(error);
        }
      }
    },
    false
  );
}
