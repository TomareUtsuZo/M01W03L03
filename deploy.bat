@echo off

REM --- Configuration ---
set IMAGE_NAME=flask-app
set CONTAINER_NAME=my-flask-app
set HOST_PORT=5000
set CONTAINER_PORT=80

REM --- Stop and Remove Existing Container (if it exists) ---
docker stop %CONTAINER_NAME% 2>nul
docker rm %CONTAINER_NAME% 2>nul

REM --- Build the Docker Image ---
echo Building Docker image: %IMAGE_NAME%
docker build -t %IMAGE_NAME% .

REM --- Run the Docker Container ---
echo Running Docker container: %CONTAINER_NAME% on port %HOST_PORT%:%CONTAINER_PORT%
docker run -d -p %HOST_PORT%:%CONTAINER_PORT% --name %CONTAINER_NAME% %IMAGE_NAME%

REM --- Test the Application ---
echo Testing the application at http://localhost:%HOST_PORT%
curl http://localhost:%HOST_PORT%

echo Deployment complete!