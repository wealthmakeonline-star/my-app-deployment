#!/bin/bash

echo "Starting deployment..."

# Backend setup
cd ../backend
pip install -r requirements.txt

# Frontend setup
cd ../frontend
npm install
npm run build

echo "Deployment complete!"