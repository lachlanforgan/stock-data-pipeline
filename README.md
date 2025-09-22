# Stock Data Pipeline

An automated data pipeline that fetches daily stock market data from Alpha Vantage API and stores it in Azure SQL Database. Built with Azure Functions for serverless execution.

## Features

- **Automated Daily Data Collection**: Timer-triggered Azure Function runs daily at 3 AM UTC
- **Multiple Stock Selection Modes**:
  - **Static**: Predefined watchlist (MSFT, AAPL, GOOGL)
  - **Dynamic**: Top 10 market gainers
  - **Hybrid**: Combines static watchlist with top 7 gainers
- **Market Movers Analysis**: Fetches top gainers, losers, and most actively traded stocks
- **Data Validation**: Ensures data integrity before database insertion
- **Duplicate Prevention**: Checks for existing records before insertion
- **Comprehensive Error Handling**: API rate limits, timeouts, and database errors
- **Secure Configuration**: Environment variables for API keys and database credentials

## Architecture

- **Azure Functions**: Serverless compute for scheduled data collection
- **Alpha Vantage API**: Real-time stock market data source
- **Azure SQL Database**: Persistent storage for historical stock data
- **Python**: Core implementation with `requests`, `pyodbc`, and `azure-functions` libraries

## Prerequisites

- Azure Functions Core Tools
- Python 3.9+
- Azure SQL Database instance
- Alpha Vantage API key (free tier available)

## Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/lachlanforgan/stockmarketvisualizer.git
   cd stockmarketvisualizer
   ```

2. **Install dependencies**
   ```bash
   pip install -r DataPipelineFunc/requirements.txt
   ```

3. **Configure environment variables**
   Create `local.settings.json` for local development:
   ```json
   {
     "IsEncrypted": false,
     "Values": {
       "ALPHAVANTAGE_API_KEY": "your_api_key_here",
       "SQL_SERVER": "your_server.database.windows.net",
       "SQL_DB": "your_database_name",
       "SQL_USER": "your_username",
       "SQL_PASSWORD": "your_password",
       "SELECTION_MODE": "hybrid"
     }
   }
   ```

4. **Database Schema**
   Create the `stock_prices` table in your Azure SQL Database:
   ```sql
   CREATE TABLE stock_prices (
       id INT IDENTITY(1,1) PRIMARY KEY,
       ticker NVARCHAR(10) NOT NULL,
       trade_date DATE NOT NULL,
       open_price DECIMAL(10,2) NOT NULL,
       high_price DECIMAL(10,2) NOT NULL,
       low_price DECIMAL(10,2) NOT NULL,
       close_price DECIMAL(10,2) NOT NULL,
       volume BIGINT NOT NULL,
       created_at DATETIME2 DEFAULT GETDATE(),
       UNIQUE(ticker, trade_date)
   );
   ```

## Configuration Options

### Stock Selection Modes

Set `SELECTION_MODE` environment variable:

- `static`: Uses predefined watchlist only
- `dynamic`: Fetches top 10 market gainers daily
- `hybrid`: Combines static watchlist with top 7 gainers (default)

### Timer Schedule

The function runs daily at 3 AM UTC. Modify the cron expression in `function.json`:
```json
{
  "schedule": "0 0 3 * * *"
}
```

## Deployment

### Azure Functions Deployment

1. **Create Function App in Azure**
2. **Deploy using Azure CLI**:
   ```bash
   func azure functionapp publish your-function-app-name
   ```
3. **Configure application settings** in Azure portal with environment variables

## API Usage

The pipeline uses Alpha Vantage's free tier:
- **Daily requests**: 500 calls/day
- **Per minute**: 5 calls/minute
- **Functions used**:
  - `TIME_SERIES_DAILY`: Individual stock data
  - `TOP_GAINERS_LOSERS`: Market movers

## Data Schema

Each stock record contains:
- `ticker`: Stock symbol (e.g., "MSFT")
- `trade_date`: Trading date
- `open_price`: Opening price
- `high_price`: Daily high
- `low_price`: Daily low
- `close_price`: Closing price
- `volume`: Trading volume

## Error Handling

- API rate limit detection and logging
- Database connection retry logic
- Data validation before insertion
- Comprehensive error logging for troubleshooting

## Monitoring

Monitor the pipeline through:
- Azure Functions logs
- Application Insights (if configured)
- Database query logs for data verification

## Development

### Local Testing

Run the standalone script for testing:
```bash
python main.py
```

### Function Development

Use Azure Functions Core Tools:
```bash
func start
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is open source. Please check the repository for license details.
