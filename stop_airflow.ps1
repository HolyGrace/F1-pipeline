# Stop Airflow Docker Compose

Write-Host "Stopping Airflow..." -ForegroundColor Yellow

docker-compose down

Write-Host ""
Write-Host "Airflow stopped successfully!" -ForegroundColor Green
Write-Host ""
Write-Host "To remove volumes (clean slate): docker-compose down -v" -ForegroundColor Yellow