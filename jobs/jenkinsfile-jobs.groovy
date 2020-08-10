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
                def input = readJSON file: '../util/job.json', returnPojo: true

                input.name = "test2";
                input.existing_cluster_id = "0810-045345-zebus756"

                writeJSON file: './job.json', json: input

                sh("cat ./job.json");

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