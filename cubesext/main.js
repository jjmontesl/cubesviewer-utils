// file my_extension/main.js

define(function(){

    function load_ipython_extension(){
        console.info('Loading CubesViewer extension for Jupyter Notebook.');
    }

    return {
        load_ipython_extension: load_ipython_extension
    };

});
