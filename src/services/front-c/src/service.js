import axios from "axios";

export const getFiles = async () =>
  await axios.get("http://localhost:9500/show-files?directory=pretransformed_data", null, {});

export const getFilesB = async () =>
  await axios.get("http://localhost:9500/show-files?directory=pretransformed_data", null, {});

export const join = async (postRes) =>
  await axios.post(process.env.REACT_APP_URI_HOST + "/start", postRes);

