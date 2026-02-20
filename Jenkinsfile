pipeline {
    agent any

    environment {
        APP_SERVER = '13.62.99.245'
        APP_SERVER_USER = 'ec2-user'
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
                dir('backend') {
                    sh '''
                        python3 -m venv venv
                        source venv/bin/activate
                        pip install -r requirements.txt || true
                        echo "Backend ready"
                    '''
                }
            }
        }

        stage('Build Frontend') {
            steps {
                dir('frontend') {
                    sh '''
                        npm install
                        npm run build
                        echo "Frontend build done"
                    '''
                }
            }
        }

        stage('Deploy') {
            steps {
                sshagent(['app-server-ssh']) {
                    sh '''
                        ssh -o StrictHostKeyChecking=no ec2-user@13.62.99.245 "
                        cd /home/ec2-user/my-app-deployment &&
                        git pull origin main &&
                        sudo systemctl restart mybackend &&
                        sudo systemctl restart nginx
                        "
                    '''
                }
            }
        }

        stage('Verify') {
            steps {
                sh '''
                    curl -f http://13.62.99.245:5000 || exit 1
                    echo Deployment OK
                '''
            }
        }

    }
}