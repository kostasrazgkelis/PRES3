import axios from "axios";

export const getFiles = async () =>
  await axios.get(process.env.REACT_APP_URI_HOST + "/show-files", null, {});

export const getJoinedFileFromHDFS = async () =>
  await axios.get("http://localhost:9500/show-files?directory=joined_data", null, {});

export const getPretransformedAFromHDFS = async () =>
  await axios.get(process.env.REACT_APP_HDFS_HOST + "/show-files?directory=cluster_a_pretransformed_data", null, {});

export const getPretransformedBFromHDFS = async () =>
  await axios.get(process.env.REACT_APP_HDFS_HOST + "/show-files?directory=cluster_b_pretransformed_data", null, {});

export const getMatchedAFromHDFS = async () =>
  await axios.get("http://localhost:9500/show-files?directory=cluster_a_matched_data", null, {});

export const getMatchedBFromHDFS = async () =>
  await axios.get(process.env.REACT_APP_HDFS_HOST + "/show-files?directory=cluster_b_matched_data", null, {});

export const uploadFiles = async (file) =>
  await axios.post(process.env.REACT_APP_URI_HOST + "/upload-file", file, {
    headers: {  "Content-Type": "multipart/form-data" }
})    
    
export const join = async (postRes) =>
  await axios.post(process.env.REACT_APP_URI_HOST + "/start", postRes);

