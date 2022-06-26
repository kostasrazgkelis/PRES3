import React from 'react'
import ToolbarWrapper from '../Toolbar/Toolbar'
import { uploadFiles } from '../../service';
import styles from './uploadFiles.module.css';
import axios from 'axios';

export default function UploadFiles() {
    const uploadFile = async(event) => {
        console.log('On upload ', event.target.files[0]);
        let file =  event.target.files[0];
        /* Response add file list to state */

        let formData = new FormData();
        formData.append('file', file);
        axios({
          url: 'http://localhost:9000/upload_files/',
          method: 'POST',
          data: formData
        }).then((res) =>{
          console.log('UPLOAD FILE RESPONSE ', res);
        }, 
        (error)=>{
          console.log('ERROR ', error);
        })
      /*   try{
          let res = await uploadFile(event.target.files[0]);
        }catch(error){
          console.log('ERROR ', error);
        } */
    }
  return (
    <ToolbarWrapper>
          <form method='post' onSubmit={uploadFile}>
            <div className={styles.UploadFilesWrapper}>
              <div className={styles.Cluster}>
                <p>Upload file for Cluster A</p>
                <input type="file" name="test1"  onLoad={uploadFile} onChange={uploadFile} />
              </div>
              
              <div>
                <p>Upload file for Cluster B</p>
                <input type="file" name="test2" onLoad={uploadFile} onChange={uploadFile} />
              </div>
          </div>
           
        </form>
    </ToolbarWrapper>
   
  )
}
