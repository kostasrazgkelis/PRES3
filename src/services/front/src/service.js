import axios from "axios";

export const getFiles = async () =>
  await axios.get(`http://localhost:9000/show_files/`, null, {});

export const uploadFiles = async (file) =>
  await axios.post(`http://localhost:9000/upload_files/`, file, {});

export const join = async (postRes) =>
  await axios.post(`http://localhost:9000/start/`, postRes);

