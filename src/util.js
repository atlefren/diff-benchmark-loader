const ms2Time = ms => {
  let secs = ms / 1000;
  ms = Math.floor(ms % 1000);
  let minutes = secs / 60;
  secs = Math.floor(secs % 60);
  let hours = minutes / 60;
  minutes = Math.floor(minutes % 60);
  hours = Math.floor(hours % 24);
  return hours + ':' + minutes + ':' + secs + '.' + ms;
};

module.exports = {ms2Time};
