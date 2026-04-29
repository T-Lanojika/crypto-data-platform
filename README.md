# Cryptocurrency Data Platform

A comprehensive Python-based platform for collecting, analyzing, and forecasting cryptocurrency data using machine learning and time series analysis. Built with real-time data from Binance and advanced technical indicators.

## 🌟 Features

### Data Collection & Storage
- **Binance Integration**: Real-time OHLCV (Open, High, Low, Close, Volume) candlestick data
- **PostgreSQL Database**: Persistent storage with optimized queries
- **Multiple Intervals**: Support for various timeframes (1m, 5m, 15m, 1h, 4h, 1d, 1w)

### Technical Analysis
- **Indicators**: ATR, EMA, MACD, RSI, Stochastic RSI, Volume Oscillator
- **Candlestick Patterns**: Recognition of standard technical patterns
- **Chart Patterns**: Trend detection and turning point analysis
- **Preprocessing**: Data normalization and gap filling

### Machine Learning & Forecasting
- **ARIMA/SARIMA Models**: Time series forecasting with seasonal decomposition
- **Deep Learning**: Neural network pipelines for trend prediction
- **Trend Analysis**: Automated trend detection across multiple timeframes
- **Pattern Pipeline**: Automated pattern recognition and classification

### Data Pipeline
- **Automated Scheduling**: APScheduler-based data collection and processing
- **Preprocessing Pipeline**: Data cleaning and feature engineering
- **Indicator Pipeline**: Automated indicator calculation
- **Pattern Pipeline**: Real-time pattern detection
- **Trend Pipeline**: Multi-timeframe trend analysis

## 📁 Project Structure

```
crypto_data_platform/
├── app/
│   ├── main.py                    # Application entry point
│   ├── config/
│   │   ├── database.py           # Database connection setup
│   │   └── settings.py           # Configuration settings
│   ├── database/
│   │   ├── base.py               # SQLAlchemy base
│   │   ├── session.py            # Database session management
│   │   └── models/
│   │       └── candlestick.py    # Candlestick data model
│   ├── indicators/               # Technical indicators
│   │   ├── atr.py
│   │   ├── ema.py
│   │   ├── macd.py
│   │   ├── rsi.py
│   │   ├── stochastic_rsi.py
│   │   └── volume_oscillator.py
│   ├── patterns/                 # Pattern recognition
│   │   ├── candlestick_patterns.py
│   │   ├── chart_patterns.py
│   │   ├── trend_detection.py
│   │   └── turning_points.py
│   ├── pipeline/                 # Data processing pipelines
│   │   ├── data_collector.py
│   │   ├── indicator_pipeline.py
│   │   ├── pattern_pipeline.py
│   │   ├── preprocessing_pipeline.py
│   │   ├── chart_pattern_pipeline.py
│   │   ├── trend_pipeline.py
│   │   ├── series_order_pipeline.py
│   │   └── scheduler.py
│   ├── services/                 # Business logic services
│   │   ├── binance_service.py
│   │   ├── data_storage_service.py
│   │   ├── gap_detection_service.py
│   │   ├── gap_filler_service.py
│   │   ├── gap_classifier_service.py
│   │   └── preprocessing_service.py
│   └── utils/
│       ├── constants.py
│       ├── helpers.py
│       └── logger.py
├── notebooks/                    # Jupyter notebooks for analysis
│   ├── arima_sarima_training.ipynb        # Time series forecasting models
│   ├── trend_analysis.ipynb
│   ├── trend_prediction_*.ipynb           # Multi-timeframe predictions
│   ├── deep_learning_preprocessing_pipeline.ipynb
│   ├── processed_datasets_overview.ipynb
│   └── crypto_data_analysis.ipynb
├── scripts/                      # Standalone execution scripts
│   ├── run_collector.py
│   ├── run_full_pipeline.py
│   ├── run_preprocessing.py
│   └── preprocess_for_deep_learning.py
├── requirements.txt              # Python dependencies
├── README.md                     # This file
└── .env                         # Environment variables (not in repo)
```

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- PostgreSQL 12+
- Git
- pip or conda

### Installation

1. **Clone the repository**
   ```bash
   git clone git@github-personal:T-Lanojika/crypto-data-platform.git
   cd crypto_data_platform
   ```

2. **Create and activate virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your settings:
   ```
   ```env
   # Database Configuration
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=crypto_data
   DB_USER=postgres
   DB_PASSWORD=your_password
   
   # Binance API
   BINANCE_API_KEY=your_api_key
   BINANCE_API_SECRET=your_api_secret
   
   # Application Settings
   LOG_LEVEL=INFO
   ```

5. **Create database**
   ```bash
   createdb crypto_data
   ```

6. **Initialize database schema**
   ```python
   python -c "from app.database.base import Base; from app.config.database import engine; Base.metadata.create_all(engine)"
   ```

## 💻 Usage

### Collect Real-Time Data
```bash
python scripts/run_collector.py --symbol BTCUSDT --interval 1h
```

### Run Full Pipeline
```bash
python scripts/run_full_pipeline.py
```

### Run Preprocessing
```bash
python scripts/run_preprocessing.py
```

### Train ARIMA/SARIMA Models
Open and run the notebook:
```bash
jupyter notebook notebooks/arima_sarima_training.ipynb
```

### Analyze Trends
```bash
jupyter notebook notebooks/trend_analysis.ipynb
```

### Start Application
```bash
python app/main.py
```

## 📊 Available Indicators

| Indicator | File | Description |
|-----------|------|-------------|
| **ATR** | `indicators/atr.py` | Average True Range - Volatility measurement |
| **EMA** | `indicators/ema.py` | Exponential Moving Average - Trend following |
| **MACD** | `indicators/macd.py` | Moving Average Convergence Divergence - Momentum |
| **RSI** | `indicators/rsi.py` | Relative Strength Index - Overbought/Oversold |
| **Stochastic RSI** | `indicators/stochastic_rsi.py` | RSI Oscillator - Additional momentum confirmation |
| **Volume Oscillator** | `indicators/volume_oscillator.py` | Volume-based momentum indicator |

## 🤖 Time Series Models

### ARIMA (AutoRegressive Integrated Moving Average)
- Non-seasonal time series forecasting
- Good for short-term predictions
- Handles trend and stationarity

### SARIMA (Seasonal ARIMA)
- Seasonal time series forecasting
- Ideal for cryptocurrency data with daily/weekly seasonality
- Captures periodic patterns

**Training Example:**
```python
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX

# Load data from database
series = load_candlestick_data('BTCUSDT', '1h', 1000)

# ARIMA model
arima = ARIMA(series, order=(1,1,1))
arima_fit = arima.fit()

# SARIMA model (with 24-hour seasonality)
sarima = SARIMAX(series, order=(1,1,1), seasonal_order=(1,1,1,24))
sarima_fit = sarima.fit()

# Forecast
forecast = sarima_fit.forecast(steps=24)
```

## 📦 Dependencies

Key packages:
- `pandas`: Data manipulation and analysis
- `sqlalchemy`: Database ORM
- `psycopg2-binary`: PostgreSQL adapter
- `numpy`: Numerical computing
- `statsmodels`: Statistical models (ARIMA, SARIMA)
- `matplotlib` & `seaborn`: Visualization
- `plotly`: Interactive charts
- `scikit-learn`: Machine learning
- `APScheduler`: Task scheduling
- `python-dotenv`: Environment variable management
- `loguru`: Advanced logging

See `requirements.txt` for complete list and versions.

## 🔧 Configuration

### Database
Configure database connection in `app/config/database.py`:
```python
DB_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
         f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
```

### Scheduler
Configure data collection frequency in `app/pipeline/scheduler.py`:
```python
scheduler.add_job(
    collect_data,
    'interval',
    minutes=5,  # Collect every 5 minutes
)
```

## 🧪 Testing

Run individual components:
```bash
python -c "from app.services.binance_service import BinanceService; BinanceService().get_klines('BTCUSDT', '1h')"
```

Verify database connection:
```python
python verify_migration.py
```

## 📚 Notebooks

- **arima_sarima_training.ipynb**: Complete ARIMA/SARIMA model training pipeline
- **trend_analysis.ipynb**: Multi-timeframe trend analysis
- **trend_prediction_*.ipynb**: Predictions for different timeframes (15m, 1h, 4h, 1d, 1w, 1MO)
- **deep_learning_preprocessing_pipeline.ipynb**: Deep learning data preparation
- **processed_datasets_overview.ipynb**: Dataset statistics and visualization

## 🔐 Security

- Never commit `.env` file - use `.env.example` instead
- Store API keys securely in environment variables
- Restrict database access to local network only
- Use strong PostgreSQL passwords

## 📝 Logging

Logging is configured with `loguru`. Check logs in:
```
logs/crypto_platform.log
```

## 🤝 Contributing

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Make your changes
3. Commit: `git commit -m "Add your feature"`
4. Push: `git push origin feature/your-feature`
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see LICENSE file for details.

## 📞 Support

For issues, questions, or suggestions:
1. Check existing GitHub issues
2. Create a new issue with detailed description
3. Include relevant logs and error messages

## 🚦 Roadmap

- [ ] Real-time WebSocket data streaming
- [ ] Advanced deep learning models (LSTM, Transformer)
- [ ] Multi-exchange support (Kraken, Coinbase)
- [ ] REST API for external integrations
- [ ] Dashboard UI (Streamlit/Dash)
- [ ] Backtesting framework
- [ ] Risk management module

## 📊 Data Sources

- **Binance**: OHLCV candlestick data via REST API
- **Extensions**: Add other exchanges as needed

## ⚡ Performance Tips

1. Use appropriate intervals for your analysis
2. Implement data caching for frequently accessed data
3. Use batch processing for large datasets
4. Monitor database indexes and query performance
5. Consider data archival for historical data

---

**Last Updated**: April 29, 2026  
**Maintainer**: T-Lanojika  
**Repository**: https://github.com/T-Lanojika/crypto-data-platform