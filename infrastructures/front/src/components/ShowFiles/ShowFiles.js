import React, { useEffect, useState } from 'react';
import ToolbarWrapper from '../Toolbar/Toolbar';
import styles from './showFiles.module.css';
import Snackbar from '@mui/material/Snackbar';
import MuiAlert from '@mui/material/Alert';
import { getFiles, join } from '../../service';
import axios from 'axios';

const Alert = React.forwardRef(function Alert(props, ref) {
  return <MuiAlert elevation={6} ref={ref} variant="filled" {...props} />;
});

export default function ShowFiles({filesA, setFilesA, filesB, setFilesB,}) {

    const [properties, setProperties] = useState({});
    const [loadingJoin, setLoadingJoin] = useState(false);
    const [results, setResults] = useState(null);
    const [open, setOpen] = useState(false);
    const [files, setFiles] = useState(null);
    
    
    const filesRes = {
        "files_a": [
            {
                "columns": [
                    "NCID",
                    "last_name",
                    "first_name",
                    "midl_name",
                    "street_name",
                    "res_city_desc",
                    "birth_age"
                ],
                "name": "100K_A.csv"
            },
            {
                "columns": [
                    "NCID",
                    "last_name",
                    "first_name",
                    "midl_name",
                    "street_name",
                    "res_city_desc",
                    "birth_age"
                ],
                "name": "50K_A.csv"
            }
        ],
        "files_b": [
            {
                "columns": [
                    "NCID",
                    "last_name",
                    "first_name",
                    "midl_name",
                    "street_name",
                    "res_city_desc",
                    "birth_age"
                ],
                "name": "100K_B.csv"
            },
            {
                "columns": [
                    "NCID",
                    "last_name",
                    "first_name",
                    "midl_name",
                    "street_name",
                    "res_city_desc",
                    "birth_age"
                ],
                "name": "50K_B.csv"
            }
        ]
    }

    useEffect(() => {
        fetchFiles();
    }, []);

    const fetchFiles = async() => {
           try{
            let res = await getFiles();
            console.log('Response ', res);
            setFiles(res);

        }catch(error){
            setFiles(filesRes)
            console.log('ERROR ', error);
        }
      
    }
    useEffect(() => {
        if(!files) return;
        let a = files['files_a'];
        let b = files['files_b'];
            
        let FAarr = [];
        let FBarr = []
        
        /* Manipulation of A files */
        for(let el of a){
            let obj = {}
            let columns = [];
            obj.name = el.name;
            obj.selected = false;
            for(let col of el.columns){
                columns.push({
                    name: col,
                    selected: true
                })
            }
            obj.columns = columns;
            FAarr.push(obj);
        }

        /* Manipulation of A files */
        for(let el of b){
            let obj = {}
            let columns = [];
            obj.name = el.name;
            obj.selected = false;
            for(let col of el.columns){
                columns.push({
                    name: col,
                    selected: true
                })
            }
            obj.columns = columns;
            FBarr.push(obj);
        }
        setFilesA(FAarr)
        setFilesB(FBarr)

        console.log('FINAL FILES ', filesA, filesB)
    }, [files]);

    const handeProperties = (event) =>{
        const { name, value } = event.target;
        console.log('HANDLE CHANGES ', name, value);
        var obj = {...properties, [name]: value}
        setProperties(obj)
    }

    const onColumnSelected = (event) =>{
        const { name, checked } = event.target;
        console.log('ON COLUMN SELECTED ', event.target.name, event.target.checked);
        /* Split the name to seperate file name from file's column */
        const nameArr = name.split("$");

        let newArr = [];
        if(nameArr[2] === 'A') newArr = [...filesA];
        else newArr = [...filesB]

        const newCols = newArr[nameArr[0]].columns.map(col => {
           /*  console.log('CONSOLE OBJ', col); */
            if(col.name === nameArr[1]){
                console.log('col name ', col.name, col.selected)
                 return {...col, selected: !col.selected};
            }
            return col;
        });
        newArr[nameArr[0]] = {...newArr[nameArr[0]], columns: newCols};
        
        if(nameArr[2] === 'A') setFilesA(newArr);
        else  setFilesB(newArr);
       
        
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

    const join = async() => {
        let postRes = {}
        let objA = {};
        let objB = {};

        for(let el of filesA){
            if(el.selected == true){
                objA.name = el.name;
                let cols = [];
                for(var col of el.columns){
                    if(col.selected) cols.push(col.name);
                }
                 objA.columns = cols;
            }
        }

         for(let el of filesB){
            if(el.selected == true){
                objB.name = el.name;
                let cols = [];
                for(var col of el.columns){
                    if(col.selected) cols.push(col.name);
                }
                objB.columns = cols;
            }
        }

        if(!objA || !objB || !properties.precision || !properties.noise){
            setOpen(true);
            return;
        }

        postRes.file_a = objA;
        postRes.file_b = objB;
        postRes.prediction_size = properties.precision;
        postRes.noise = properties.noise;
        postRes.matching_field = 'NCID';

        /* POST HERE */
        console.log('Post Res', postRes);
        try{
            /* let res = await join(postRes);
            console.log('Response ', res); */
            const response = await axios.post('http://localhost:9000/start/', postRes);
            console.log('RESPONSE ', response);
            setResults(response);

        }catch(error){
            console.log('ERROR ', error);
        }
    }

    const handleClose = (event, reason) => {
        if (reason === 'clickaway') {
        return;
        }

        setOpen(false);
    };

    const displayAFiles = filesA?.map((item, idx) => {
        return (
            <div className={styles.FileWrapper} key={idx}>
                <div className={styles.FileItem} >
                    <p>{item.name}</p>
                    <div className={styles.Columns}>
                        {item.columns.map((column, index) => { return (
                            <div key={index}>
                                <p className='pLight'>{column.name}</p>
                                <input type="checkbox" name={`${idx}$${column.name}$A`} checked={column.selected} onChange={onColumnSelected}/>
                            </div>
                        )})}
                    </div>
                </div>
                <input type="checkbox" name={`${item.name}`} checked={item.selected} onChange={onFileSelectedA}/>
            </div>
        )
    })

    const displayBFiles = filesB?.map((item, idx) => {
        return (
            <div className={styles.FileWrapper} key={idx}>
                <div className={styles.FileItem} >
                    <p>{item.name}</p>
                    <div className={styles.Columns}>
                        {item.columns.map((column, index) => {return (
                            <div key={index}>
                                <p className='pLight'>{column.name}</p>
                                <input type="checkbox" name={`${idx}$${column.name}$B`} checked={column.selected} onChange={onColumnSelected}/>
                            </div>
                        )})}
                    </div>
                </div>
                <input type="checkbox" name={`${item.name}`} checked={item.selected} onChange={onFileSelectedB}/>
            </div>
        )
    })
    
    return (
        <ToolbarWrapper>
            {!results ? <div>
                <div>
                    <p className={styles.MarginBottom}>Files A</p>
                    {displayAFiles}
                </div>

                <div className={styles.MarginTop}>
                    <p className={styles.MarginBottom}>Files B</p>
                    {displayBFiles} 
                </div>

                <div className={styles.MarginTopSmall}>
                    <input placeholder='Add precision' name='precision' type='number' step={0.05} onChange={handeProperties}/>
                    <input placeholder='Add noise' name='noise' type='number' onChange={handeProperties}/>
                </div>
              
                <button className={styles.MarginTopXSmall} onClick={join}>JOIN</button>
                
                <Snackbar open={open} autoHideDuration={4000} onClose={handleClose}>
                    <Alert onClose={handleClose} severity="error" sx={{ width: '100%' }}>
                    You must selected one file of each category and fill the prediction size and noise!
                    </Alert>
                </Snackbar>
            </div>:
            <div>
                <p>Size: {results.size}</p>
                <p>Prediction: {results.prediction}</p>
                <p>Precision: {results.precision}</p>
                <p>Recall: {results.recall}</p>
                <p>TP: {results.TP}</p>
                <p>FP: {results.FP}</p>
                <p>Total Matches: {results.total_matches}</p>
                <p>Noise: {results.nois}</p>
            </div>
            
            }
            
        </ToolbarWrapper>
    )
}
