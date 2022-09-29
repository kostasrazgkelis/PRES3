import React, { useState } from 'react'
import fileDownload from 'js-file-download'

import ToolbarWrapper from '../Toolbar/Toolbar';
import styles from "./requestMatchedData.module.css";
import axios from 'axios';

export default function Home() {

    const [results, setResults] = useState(null);
    const NAME_OF_OTHER_CLUSTER = process.env.REACT_APP_OTHER_CLUSTER;
    const FileDownload = require('js-file-download');

    const matchedData = [
            {"file_name": NAME_OF_OTHER_CLUSTER + "_matched_files_1.csv"},
            {"file_name": NAME_OF_OTHER_CLUSTER + "_matched_files_2.csv"},
        ]

    const handleClickDownload = async () => {
        const data = {
            "matching_field": "NCID",
            "joined_data_filename": "file",
            "file_name": "file",

        }
        try{
            const response = await axios.get(process.env.REACT_APP_OTHER_CLUSTER_URL + "/send-data/", { params: { data } });
            FileDownload(response.data, 'download.csv')
            setResults(response);
        }catch(error){
            console.log('ERROR ', error);
        }
    };

    const displayMatchedData = matchedData?.map((item, idx) => {
        return (
            <div className={styles.FileWrapper} key={idx}>
                <div className={styles.FileItem} >
                    <p>{item.file_name}</p>
                    <div>
                    <button className={styles.MarginTopXSmall} onClick={() => {
                        handleClickDownload()}}>Download</button>
                    </div>
                </div>
            </div>
        )
    })

  return (
    <ToolbarWrapper>
        <h2> Request matched data from {NAME_OF_OTHER_CLUSTER} </h2>
        <div>
            <div className={styles.MarginTop}>
                {displayMatchedData}
            </div>
        </div>
    </ToolbarWrapper>

  )
}
