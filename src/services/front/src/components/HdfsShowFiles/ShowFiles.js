import React, { useEffect, useState } from 'react';
import ToolbarWrapper from '../Toolbar/Toolbar';
import styles from './showFiles.module.css';
import Snackbar from '@mui/material/Snackbar';
import MuiAlert from '@mui/material/Alert';
import {getJoinedFileFromHDFS, getMatchedAFromHDFS, getPretransformedAFromHDFS} from '../../service';
import axios from "axios";

const Alert = React.forwardRef(function Alert(props, ref) {
  return <MuiAlert elevation={6} ref={ref} variant="filled" {...props} />;
});

export default function HdfsShowFiles({filesA, setFilesA, filesB, setFilesB}) {

    const [properties, setProperties] = useState({});
    const [loadingJoin, setLoadingJoin] = useState(false);
    const [results, setResults] = useState(null);
    const [open, setOpen] = useState(false);
    const [files, setFiles] = useState(null);
    const NAME_OF_CLUSTER = process.env.REACT_APP_NAME_OF_CLUSTER;
    const NAME_OF_CLUSTER_URL = process.env.REACT_APP_NAME_OF_CLUSTER_URL;

    const filesRes = {
        "joined_data": ["joined_files.csv"],
        "matched_data": ["matched_files.csv"],
    }

    const MockJoinedFiles = [
            {"file_name": "joined_files_1.csv"},
            {"file_name": "joined_files_2.csv"},
        ]

    const MockMatchedFiles = [
            {"file_name": NAME_OF_CLUSTER_URL + "_matched_files_1.csv"},
            {"file_name": NAME_OF_CLUSTER_URL + "_matched_files_2.csv"},
        ]

    const MockTransformedFiles = [
            {"file_name": NAME_OF_CLUSTER_URL + "_pretransformed_files_1.csv"},
            {"file_name": NAME_OF_CLUSTER_URL + "_pretransformed_files_2.csv"},
        ]

    useEffect(() => {
        fetchFiles();
    }, []);

    const fetchFiles = async() => {
        try{
            axios.all([getJoinedFileFromHDFS(),
                       getMatchedAFromHDFS()]).then(
                axios.spread((...allData) => {
                    const allJoinedData = allData[0].data
                    const allMatchedData = allData[1].data
                    setFiles(filesRes)

            })
        )

        }catch(error){
            setFilesA(MockJoinedFiles)
            setFilesB(MockMatchedFiles)
            console.log('ERROR ', error);
        }
      
    }
    useEffect(() => {
        if(!files) return;

        let a = files['joined_data'];
        let b = files['matched_data'];

        let FAarr = [];
        let FBarr = [];

        /* Manipulation of A files */
        for(let el of a){
            let obj = {}
            obj.name = el.name;
            FAarr.push(obj);
        }

        for(let el of b){
            let obj = {}
            obj.name = el.name;
            FBarr.push(obj);
        }
        setFilesA(FAarr)
        setFilesB(FBarr)

    }, []);


    const handeProperties = (event) =>{
        const { name, value } = event.target;
        console.log('HANDLE CHANGES ', name, value);
        var obj = {...properties, [name]: value}
        setProperties(obj)
    }

    const onFileSelectedA = (event) => {
        const { name, checked } = event.target;

        const newArr = filesA.map((obj) => {
            if (obj.name === name) {
                return {...obj, selected: checked};
            }
            else{
                return {...obj, selected: false};
            }
        });
        setFilesA(newArr);
    }

    const onFileSelectedB = (event) => {
        const { name, checked } = event.target;

        const newArr = filesB.map((obj) => {
            if (obj.name === name) {
                return {...obj, selected: checked};
            }
            else{
                return {...obj, selected: false};
            }
        });

        setFilesB(newArr);
    }

    const handleClose = (event, reason) => {
        if (reason === 'clickaway') {
        return;
        }

        setOpen(false);
    };

    const handleClickDownload = async (directory, file) => {
        const postRes = {
            "file": file
        }
        try{
            const response = await axios.get(process.env.REACT_APP_URI_HOST + "/take-file/" + directory, postRes);
            setResults(response);
        }catch(error){
            console.log('ERROR ', error);
        }
    };

    const displayJoinedFiles = MockJoinedFiles?.map((item, idx) => {
    return (
        <div className={styles.FileWrapper} key={idx}>
            <div className={styles.FileItem} >
                <p>{item.file_name}</p>
                <div>
                    <button className={styles.MarginTopXSmall} onClick={() => {
                        handleClickDownload("joined_data", item.file_name)}}>Download</button>
                </div>
            </div>
        </div>
        )
    })

    const displayMatchedData = MockMatchedFiles?.map((item, idx) => {
    return (
        <div className={styles.FileWrapper} key={idx}>
            <div className={styles.FileItem} >
                <p>{item.file_name}</p>
                <div>
                    <button className={styles.MarginTopXSmall} onClick={() => {
                        handleClickDownload(NAME_OF_CLUSTER_URL + "_matched_data", item.file_name)}}>Download</button>
                </div>
            </div>
        </div>
        )
    })

    const displayTransformedData = MockTransformedFiles?.map((item, idx) => {
    return (
        <div className={styles.FileWrapper} key={idx}>
            <div className={styles.FileItem} >
                <p>{item.file_name}</p>
                <div>
                    <button className={styles.MarginTopXSmall} onClick={() => {
                        handleClickDownload("_pretransformed_data", item.file_name)}}>Download</button>
                </div>
            </div>
        </div>
        )
    })
    return (
        <ToolbarWrapper>
            {loadingJoin? <h2>LOADING RESULTS</h2> : <> {!results ? <div>
                <div>
                    <p className={styles.MarginBottom}>Joined Data</p>
                        {displayJoinedFiles}
                </div>
                <div>
                    <p className={styles.MarginBottom}>Matched {NAME_OF_CLUSTER} Data</p>
                        {displayMatchedData}
                </div>
                <div>
                    <p className={styles.MarginBottom}>Transformed {NAME_OF_CLUSTER} Data</p>
                        {displayTransformedData}
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
