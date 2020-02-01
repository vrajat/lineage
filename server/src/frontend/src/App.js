import React from 'react';
import './App.css';
import Layout from "./components/layout";
import Dag from "./components/dag";

function App() {
  return (
      <React.Fragment>
        <Layout>
          <Dag/>
        </Layout>
      </React.Fragment>
  );
}

export default App;
