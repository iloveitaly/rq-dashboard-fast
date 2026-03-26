function initAutorefresh(updateFn, protocol, prefix) {
  var host = window.location.host;
  var baseurl = protocol + '://' + host + prefix;

  var timer = null;
  var lastResponseTime = 0;
  var MIN_MS = 5000;
  var MAX_MS = 60000;

  function adaptiveInterval() {
    if (lastResponseTime === 0) return MIN_MS;
    return Math.min(MAX_MS, Math.max(MIN_MS, lastResponseTime * 3));
  }

  function scheduleNextRefresh() {
    clearTimeout(timer);
    if (!window.autoRefreshEnabled) return;
    timer = setTimeout(updateFn, adaptiveInterval());
  }

  function recordResponseTime(ms) {
    lastResponseTime = ms;
  }

  function startAutorefresh() {
    scheduleNextRefresh();
  }

  function stopAutorefresh() {
    clearTimeout(timer);
    timer = null;
  }

  return {
    baseurl: baseurl,
    scheduleNextRefresh: scheduleNextRefresh,
    recordResponseTime: recordResponseTime,
    startAutorefresh: startAutorefresh,
    stopAutorefresh: stopAutorefresh,
  };
}
