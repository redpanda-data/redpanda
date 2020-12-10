const createTitleFromName = (name: string) => {
  return name.replace("vectorized", "").replace(/_/g, " ");
};
export default createTitleFromName;
