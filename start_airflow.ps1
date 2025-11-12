# Start Airflow with Docker Compose

Write-Host "Starting Airflow with Docker..." -ForegroundColor Green

# Check if Docker is running
$dockerRunning = docker ps 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Docker is not running. Please start Docker Desktop first." -ForegroundColor Red
    exit 1
}

# Create necessary directories
Write-Host "Creating directories..." -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path ".\logs" | Out-Null
New-Item -ItemType Directory -Force -Path ".\plugins" | Out-Null

# Set AIRFLOW_UID for Linux compatibility
$env:AIRFLOW_UID = "50000"

# Start Docker Compose
Write-Host "Starting Airflow containers..." -ForegroundColor Yellow
docker-compose up -d

# Wait for services to be ready
Write-Host "Waiting for services to start (this may take 1-2 minutes)..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# Check status
Write-Host ""
Write-Host "Checking container status..." -ForegroundColor Yellow
docker-compose ps

Write-Host ""
Write-Host "Airflow is starting up!" -ForegroundColor Green
Write-Host ""
Write-Host "Access Airflow UI at: http://localhost:8080" -ForegroundColor Cyan
Write-Host "Username: admin" -ForegroundColor Cyan
Write-Host "Password: admin" -ForegroundColor Cyan
Write-Host ""
Write-Host "To view logs:    docker-compose logs -f" -ForegroundColor Yellow
Write-Host "To stop Airflow: docker-compose down" -ForegroundColor Yellow