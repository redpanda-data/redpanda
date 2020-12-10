import React, { useState, useEffect } from "react";

// Default interval from promethus snapshots - check prometheus output + choose names
const initialInterval = 2500;

const useInterval = () => {
  const defaultInterval = localStorage.getItem("defaultInterval");
  const [interval, setInterval] = useState(
    defaultInterval ? parseInt(JSON.parse(defaultInterval)) : initialInterval
  );
  const [count, setCount] = useState(0);

  const updateInterval = (data: number) => {
    setInterval(data);
    localStorage.setItem("defaultInterval", JSON.stringify(data));
  };

  const changeInterval = (event: any) => {
    updateInterval(parseInt(event?.target?.value));
  };

  const resetInterval = () => {
    updateInterval(initialInterval);
  };

  useEffect(() => {
    const timer = setTimeout(() => setCount(count + 1), interval);
    return () => clearTimeout(timer);
  }, [count, setCount]);

  return {
    count,
    setCount,
    interval,
    setInterval,
    changeInterval,
    resetInterval,
  };
};

export default useInterval;
