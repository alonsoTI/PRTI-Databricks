/* Project 4 letters. */ 
def project                 = 'BMDL'
def deploymentEnvironment   = 'dev'
def appName                 = "databricks-cli";
def appVersion              = "2.0";
def AzureResourceName       = "prueba";
def azureWebApp             = "demo-app-inct";
def dockerRegistryUrl       = "vlliuyadesa.azurecr.io";
def imageTag                = "vlliuyadesa.azurecr.io/databricks-cli:1.2";
def databricksHost          = "https://adb-5686748607945778.18.azuredatabricks.net/";
def databricksToken2        = "dapi3876d50d470fe89da3d163d33757ec58";
def databricksContainer     = "";

/* Mail configuration*/
// If recipients is null the mail is sent to the person who start the job
// The mails should be separated by commas(',')
//def recipients

try {
   node { 
      stage('Pull Source Code') {
        checkout([$class: 'GitSCM', branches: [[name: '*/master']], 
        doGenerateSubmoduleConfigurations: false, 
        extensions: [], 
        submoduleCfg: [], 
        userRemoteConfigs: [[credentialsId: 'github-jenkins', 
        url: 'https://github.com/alonsoTI/PRTI-Databricks.git']]])
      }

      stage('Prepare Notebooks') {
        steps.echo """
        ******** Read Notebooks  ********
        """
        steps.sh "ls notebooks"
        sh("echo [DEFAULT] > $WORKSPACE/databricks.cfg")
		        sh("echo host = ${databricksHost} >> $WORKSPACE/databricks.cfg")
		        sh("echo token = ${databricksToken2} >> $WORKSPACE/databricks.cfg")
      }

      stage('QA Analisys') {
        steps.echo """
        ******** Analiza con Sonar ********
        """
      }
     
      stage('SAST Analisys') {
        steps.echo """
        ******** Analiza con Fortify********
        """
      }

      stage('Save Artifact') {
        steps.echo """
        ******** Almacena en Artifactory ********
        """
      }

      stage("Deploy to " +deploymentEnvironment){
        steps.withCredentials([
                [$class: "StringBinding", credentialsId: "tenantId", variable: "tenantId" ],
                [$class: "StringBinding", credentialsId: "databricksToken", variable: "databricksToken" ]
            ]){
                try{
                    sh("DATABRICKS_CONFIG_FILE=$WORKSPACE/databricks.cfg databricks workspace list")
                    
                    steps.echo """
                    ******** Validando existencia de directorios********
                    """
                    
                    def dirs = readJSON file: 'notebooks/notebook.json', returnPojo: true
                    dirs.each{ nombre ->
                        steps.echo """ Ruta:  ${nombre.ruta}"""
                        try{
                        def dir = readJSON text: sh(script: "DATABRICKS_CONFIG_FILE=$WORKSPACE/databricks.cfg databricks workspace list ${nombre.ruta} | awk '{ print \$2 \$NF}'", returnStdout: true)
                        steps.echo """ La ruta:  ${dir} si existe"""
                        
                        if (dir.error_code == "RESOURCE_DOES_NOT_EXIST"){
                            steps.echo """ La ruta:  ${nombre.ruta} no existe, ejecutando creación: """
                            sh("DATABRICKS_CONFIG_FILE=$WORKSPACE/databricks.cfg databricks workspace mkdirs ${nombre.ruta}")
                        }
                      }catch (Exception e){
                         steps.echo """ La ruta:  ${nombre.ruta} si existe"""
                      }
                    }
                    

                    steps.echo """
                    ******** Importando notebooks en Databricks DEV********
                    """
                    def props = readJSON file: 'notebooks/notebook.json', returnPojo: true
                    props.each{ nombre ->
                        steps.echo """Cargando notebook ${nombre.nombre} a la ruta ${nombre.ruta}"""
                        sh("DATABRICKS_CONFIG_FILE=$WORKSPACE/databricks.cfg databricks workspace import -l PYTHON notebooks/${nombre.nombre} ${nombre.ruta}/${nombre.nombre} -o")
                    }
                    
                }catch(Exception e){
                throw e;
                }
            }
      }

      stage('Post Execution') {
        
      }
   }
} catch(Exception e) {
   node {
    throw e
   }
}