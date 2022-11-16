import axios from "axios";


export const getFiles = async () =>
  await axios.get(process.env.REACT_APP_URI_HOST + "/show-files", null, {});

export const getJoinedFileFromHDFS = async () =>
  await axios.get("http://localhost:9500/show-files?directory=joined_data", null, {});

export const getPretransformedAFromHDFS = async () =>
  await axios.get("http://localhost:9500/show-files?directory=pretransformed_data", null, {});

export const getMatchedAFromHDFS = async () =>
  await axios.get("http://localhost:9500/show-files?directory=joined_data", null, {});

export const uploadFiles = async (file) =>
  await axios.post(process.env.REACT_APP_URI_HOST + "/upload-file", file, {
    headers: {  "Content-Type": "multipart/form-data" }
})

export const join = async (postRes) =>
  await axios.post(process.env.REACT_APP_URI_HOST + "/start", postRes);

