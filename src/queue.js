const queue = limit => {
  const funcs = [];
  let numRunning = 0;

  const checkStart = () => {
    if (numRunning < limit) {
      if (funcs.length === 0) {
        return;
      }
      const [f, t, e] = funcs.shift();
      numRunning++;
      f()
        .then((...args) => {
          t(...args);
          numRunning--;
          checkStart();
        })
        .catch(err => {
          e(err);
          numRunning--;
          checkStart();
        });
    }
  };

  return {
    add: (func, then, err) => {
      funcs.push([func, then, err]);
      checkStart();
    }
  };
};

const getCaller = func => (...args) => () => func(...args);

module.exports = {queue, getCaller};
