# Crypto Collectors

A comprehensive web application for cryptocurrency market analysis that collects, processes, and analyzes historical data from major cryptocurrency exchanges.

## Links

- Hosted on Microsoft Azure:  https://cryptocollectors-hhcse6gdhwemedch.norwayeast-01.azurewebsites.net/home
- Microservice hosted on Microsoft Azure: https://kriptoservis-h3f8hkcce8d8dqf3.norwayeast-01.azurewebsites.net/docs
- Video overview of the entire architecture: https://github.com/MartinaIvanovska/crypto_collectors/blob/main/video/video.mp4

## Overview

Crypto Collectors is a sophisticated platform designed for cryptocurrency market analysis. The application aggregates historical data from major exchanges for the top 1000 active cryptocurrencies, spanning the last 10 years of daily data. The platform delivers advanced analytical capabilities including technical analysis indicators, LSTM-based price predictions, and integrated on-chain with sentiment analysis.


## Technologies

### Backend
- **Java Spring Boot** - Main web application framework
- **Python** - Microservices and LSTM prediction models
- **PostgreSQL** - Primary database for cryptocurrency data storage

### Data Processing
- **Pipe and Filter Architecture** - For automated data downloading and transformation
- **Pandas** - For technical indicator calculations
- **TensorFlow** - For LSTM neural network implementation

### APIs and Microservices
- **RESTful APIs** - Communication between microservices and main application
- **External APIs** - Yahoo Finance API and sentiment analysis platforms

### Frontend
- **Thymeleaf** - UI framework (as implemented in mockups)

### Deployment
- **Docker** - Containerization
- **Azure Web Apps** - Cloud deployment

## Features

- Historical data collection from major cryptocurrency exchanges
- Technical analysis indicators calculation
- LSTM-based price prediction models
- On-chain data integration
- Sentiment analysis integration
- Interactive dashboard with Thymeleaf UI
