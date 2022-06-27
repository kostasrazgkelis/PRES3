import './App.css';
import { useState } from 'react';
import ShowFiles from "./components/ShowFiles/ShowFiles.js";
import UploadFiles from "./components/UploadFiles/UploadFiles.js";
import Home from './components/Home/Home';
import {
  BrowserRouter as Router, 
  Routes, 
  Route
} from 'react-router-dom';

function App() {
  const [files, setFiles] = useState([
    {
      fileName: 'File 1',
      columns: ['Column A', 'Column B']
    },
    {
      fileName: 'File 2',
      columns: ['Column A', 'Column B', 'Column C']
    },
    {
      fileName: 'File 3',
      columns: ['Column A']
    }
  ]);
  const [filesA, setFilesA] = useState(null);
  const [filesB, setFilesB] = useState(null);

  const uploadCompletedFiles = () => {
    /* State */

    /* Response add file list to state */
  }

  return (
    <div className="App">
      <Router>
        <Routes>
          <Route path="/" element={<Home/>} />
          <Route path="/all-files" element={<ShowFiles filesA={filesA} setFilesA={setFilesA} filesB={filesB} setFilesB={setFilesB}/>} />
          <Route path="/upload-files" element={<UploadFiles/>} />
        </Routes>
    </Router>
    </div>
  );
}

export default App;
