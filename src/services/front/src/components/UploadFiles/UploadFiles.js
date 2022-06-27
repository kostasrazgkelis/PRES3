import React, {useState} from 'react'
import ToolbarWrapper from '../Toolbar/Toolbar'
import { uploadFiles } from '../../service';
import styles from './uploadFiles.module.css';

export default function UploadFiles() {
    const [fileA, setFileA] = useState(null);
    const [fileB, setFileB] = useState(null);
    
    const uploadFileA = async(event) => {
        console.log('On upload ', event.target.files[0]);
        let file =  event.target.files[0];
        setFileA(file);

    }

    const uploadFileB = async(event) => {
      console.log('On upload ', event.target.files[0]);
      let file =  event.target.files[0];
      setFileB(file);

  }

    const uploadAllFiles = async(event) => {
        event.preventDefault();
        let formData = new FormData();
        formData.append('uploadedFile_A', fileA);
        formData.append('uploadedFile_B', fileB);

        try{
            const response = await uploadFiles(formData);
            console.log('RESPONSE', response);
        }catch(error){
            console.log('ERROR ', error);
        }

    }

  return (
    <ToolbarWrapper>
          <form method='post' >
            <div className={styles.UploadFilesWrapper}>
              <div className={styles.Cluster}>
                <p>Upload file for Cluster A</p>
                <input type="file" name="uploadedFile_A" onChange={uploadFileA} onLoad={uploadFileA}  />
              </div>
              
              <div>
                <p>Upload file for Cluster B</p>
                <input type="file" name="uploadedFile_B" onChange={uploadFileB} onLoad={uploadFileB}  />
              </div>
              
          </div>
          <div>
              <button type="submit" onClick={uploadAllFiles}>Upload All Files</button>
          </div>
           
        </form>
    </ToolbarWrapper>
  )
}
