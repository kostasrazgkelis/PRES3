import React, { useState, useEffect } from 'react'
import { getMatchedAFromHDFS } from '../../service';
import ToolbarWrapper from '../Toolbar/Toolbar';
import styles from "./requestMatchedData.module.css";
import axios from 'axios';
import Snackbar from '@mui/material/Snackbar';
import MuiAlert from '@mui/material/Alert';

const Alert = React.forwardRef(function Alert(props, ref) {
  return <MuiAlert elevation={6} ref={ref} variant="filled" {...props} />;
});

export default function RequestMatchedData({matchedFiles, setMatchedFiles}) {

    const [results, setResults] = useState(null);
    const [open, setOpen] = useState(false);
    const [loadingJoin, setLoadingJoin] = useState(false);
    const NAME_OF_OTHER_CLUSTER = process.env.REACT_APP_OTHER_CLUSTER;
    const FileDownload = require('js-file-download');
    const [files, setFiles] = useState(null)

    const filesRes = [
            {"file_name": NAME_OF_OTHER_CLUSTER + "_matched_files_1.csv"},
            {"file_name": NAME_OF_OTHER_CLUSTER + "_matched_files_2.csv"},
        ]


    useEffect(() => {
        fetchFiles();
    }, []);

    const fetchFiles = async () => {
        try{
            await axios.all([getMatchedAFromHDFS()]).then(
                axios.spread((...allData) => {
                    setFiles(allData)
                    console.log('DATA from APi ', files);

                })
            )
        }catch(error){
            setFiles(filesRes)
            console.log('ERROR ', error);
        }

    }
    useEffect(() => {
        if (!files) return;

        let matched_data_array = [];

        if (files[0].data.files.length !== 0)
            for(let el of files[0].data.files){
                let obj = {}
                obj.name = el.name;
                matched_data_array.push(obj);
            }

        setMatchedFiles(matched_data_array)

    }, [files]);

    const handleClose = (event, reason) => {
        if (reason === 'clickaway') {
        return;
        }

        setOpen(false);
    };

    const handleClickDownload = async (directory, file ) => {
        const data = {
            "matching_field": "NCID",
            "joined_data_filename": "file",
            "file_name": file,
        }
        try{
            const response = await axios.get(process.env.REACT_APP_OTHER_CLUSTER_URL + "/send-data/", { params: { data } });
            FileDownload(response.data, 'download.csv')
            setResults(response);
        }catch(error){
            console.log('ERROR ', error);
        }
    };

    const displayMatchedData = matchedFiles?.map((item, idx) => {
        return (
            <div className={styles.FileWrapper} key={idx}>
                <div className={styles.FileItem} >
                    <p>{item.name}</p>
                    <div>
                    <button className={styles.MarginTopXSmall} onClick={() => {
                        handleClickDownload("matched_data", item.name)}}>Download</button>
                    </div>
                </div>
            </div>
        )
    })

  return (
    <ToolbarWrapper>
        {loadingJoin? <h2> Request matched data from {NAME_OF_OTHER_CLUSTER} </h2> : <> {!results ? <div>
        <div>
            <p className={styles.MarginTop}> Results </p>
            {displayMatchedData}
        </div>
        <Snackbar open={open} autoHideDuration={4000} onClose={handleClose}>
        <Alert onClose={handleClose} severity="error" sx={{width: '100%'}}>
        You must selected one file of each category and fill the prediction size and noise!
        </Alert>
        </Snackbar>
        </div>:
        <div>
        </div>
        }</>}
    </ToolbarWrapper>

  )
}
