pipeline {
    agent any
    
    environment {
        APP_SERVER = '13.60.185.87'
        SSH_USER = 'ec2-user'
        DEPLOY_DIR = '/home/ec2-user/my-app-deployment'
    }
    
    stages {
        
        stage('Checkout Code') {
            steps {
                git branch: 'main',
                url: 'https://github.com/wealthmakeonline-star/my-app-deployment.git'
            }
        }
        
        stage('Test Backend') {
            steps {
                sh '''
                cd backend
                
                python3 -m venv venv
                . venv/bin/activate
                
                pip install flask flask-cors pytest
                
                pytest || true
                '''
            }
        }
        
        stage('Build Frontend') {
            steps {
                sh '''
                cd frontend
                
                npm install
                
                npm run build
                '''
            }
        }
        
        stage('Deploy') {
            steps {
                sshagent(['app-server-ssh']) {
                    sh '''
                    
                    ssh -o StrictHostKeyChecking=no ec2-user@13.60.185.87 "
                    
                    cd /home/ec2-user/my-app-deployment
                    
                    git pull origin main
                    
                    sudo systemctl restart mybackend
                    
                    sudo systemctl restart myfrontend
                    
                    "
                    
                    '''
                }
            }
        }
        
        stage('Verify') {
            steps {
                sh '''
                
                curl -f http://13.60.185.87 || exit 1
                
                '''
            }
        }
        
    }
}
