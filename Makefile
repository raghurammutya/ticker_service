build:
\tdocker build -t ticker_service:dev -f docker/Dockerfile .

refresh:
\tdocker build -t ticker_service:dev -f docker/Dockerfile .
\tkind load docker-image ticker_service:dev

deploy:
\tkubectl apply -f kubernetes/ticker_service.yaml
