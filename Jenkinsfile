node {
    try {
        stage 'Build'

        deleteDir()
        
        checkout scm

        def workspace = pwd() 
        def version = env.BRANCH_NAME

        if(env.BRANCH_NAME=="master") {
            version = sh(returnStdout: true, script: 'git describe --abbrev=0 --tags').trim()
            checkout([$class: 'GitSCM', branches: [[name: "tags/${version}"]], extensions: [[$class: 'CleanCheckout']]])
        }

        sh """
            mvn versions:set -DnewVersion=${version}
            mvn clean package
            tar uf target/platform-testing-cdh-${version}.jar 
            tar uf target/platform-testing-general-${version}.jar 
        """

        stage 'Test'
        sh '''
        '''

        stage 'Deploy' 

        build job: 'deploy-component', parameters: [[$class: 'StringParameterValue', name: 'branch', value: env.BRANCH_NAME],[$class: 'StringParameterValue', name: 'component', value: "platform-testing"],[$class: 'StringParameterValue', name: 'release_path', value: "platform/releases"],[$class: 'StringParameterValue', name: 'release', value: "${workspace}/target/platform-testing-cdh-${version}.tar.gz"]]

        build job: 'deploy-component', parameters: [[$class: 'StringParameterValue', name: 'branch', value: env.BRANCH_NAME],[$class: 'StringParameterValue', name: 'component', value: "platform-testing"],[$class: 'StringParameterValue', name: 'release_path', value: "platform/releases"],[$class: 'StringParameterValue', name: 'release', value: "${workspace}/target/platform-testing-general-${version}.tar.gz"]]
    
        emailext attachLog: true, body: "Build succeeded (see ${env.BUILD_URL})", subject: "[JENKINS] ${env.JOB_NAME} succeeded", to: "${env.EMAIL_RECIPIENTS}"
    }
    catch(error) {
        emailext attachLog: true, body: "Build failed (see ${env.BUILD_URL})", subject: "[JENKINS] ${env.JOB_NAME} failed", to: "${env.EMAIL_RECIPIENTS}"
        currentBuild.result = "FAILED"
        throw error
    }

}
