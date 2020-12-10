const fetchPrometheusSnapshot = async (ip_address: string = "127.0.0.1") => {
  const url = `http://${ip_address}:9644/metrics`;
  let response = await fetch(url, { mode: "no-cors" });
  // let snapshots = await response.text()
  return response;
};

export default fetchPrometheusSnapshot;
