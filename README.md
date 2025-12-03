# Portfolio Reporting Data Sync

A simple tool to fetch data from Kaia Solutions Portal API and prepare it for PowerBI reporting.

## What Does This Do?

This tool automatically downloads data from your Kaia Solutions Portal and saves it to a database file that PowerBI can read. It fetches:

- 📊 **Companies** - Your company information
- 🏭 **Power Plants** - All your power plant details
- ⚡ **Production Data** - Daily production records
- 💰 **Market Prices** - Electricity market prices by area
- 🔧 **Downtime Events** - Maintenance and downtime tracking
- 📝 **Work Items** - O&M work items and tasks

The data is saved to a SQLite database file (`data/portfolio_report.db`) that PowerBI can connect to directly.

## Prerequisites

You need:
- Python 3.10 or newer installed on your computer
- Access to Kaia Solutions Portal API (you'll need an API key)
- Internet connection to reach the API

## Installation

### Option 1: Using pip (Recommended)

1. Open a terminal/command prompt
2. Navigate to this directory:
   ```bash
   cd portfolio_reporting
   ```

3. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

### Option 2: Using Poetry

If you have Poetry installed:

```bash
cd portfolio_reporting
poetry install
```

## Configuration

Before running the sync, you need to configure your API connection:

1. Copy the example configuration file:
   ```bash
   cp config.yaml.example config.yaml
   ```

2. Edit `config.yaml` with your settings:
   ```yaml
   api:
     base_url: "http://localhost:3000"  # Your Kaia Solutions Portal URL
     api_key: "your_api_key_here"       # Get this from Kaia Solutions Portal
     timeout: 30
     retry_attempts: 3

   database:
     path: "data/portfolio_report.db"   # Where to save the data

   data:
     start_date: "2025-01-01"           # Only fetch data from this date
     # end_date: "2025-12-31"           # Optional: limit end date
   ```

3. **Important:** Replace `your_api_key_here` with your actual API key from Kaia Solutions Portal

## How to Run

### Full Sync (Download All Data)

To download all data from the configured start date:

```bash
python -m portfolio_reporting --mode full
```

This will:
- Create/update the database at `data/portfolio_report.db`
- Download all data from your configured start date
- Show a summary when complete

### Incremental Sync (Update Only New Data)

To only download data that changed since the last sync:

```bash
python -m portfolio_reporting --mode incremental
```

This is faster and should be used for regular updates.

## Verifying Your Data

After running a sync, you can verify what data was downloaded:

```bash
python verify_data.py
```

This shows a summary like:
```
================================================================================
PORTFOLIO REPORTING DATA VERIFICATION
================================================================================

📊 Companies: 7
🏭 Power plants: 9
⚡ Production days: 2,704
   └─ Date range: 2025-01-01 to 2025-10-31
💰 Market prices: 172,826
   └─ Time range: 2025-01-01T00:00:00.000+01:00 to 2025-10-27T22:00:00.000+01:00
   └─ Price areas: 29
🔧 Downtime events: 100
📝 Work items: 158
   └─ Created range: 2022-11-11T10:16:35+01:00 to 2025-10-22T11:24:12+02:00
```

## Connecting PowerBI

1. Open PowerBI Desktop
2. Click **Get Data** → **More...**
3. Search for **SQLite** and select it
4. Browse to the database file:
   - Default location: `data/portfolio_report.db` in this folder
5. Select the tables you want to use in your report
6. Click **Load**

### Available Tables

The database contains these tables:

| Table Name | Description |
|------------|-------------|
| `companies` | Company information |
| `power_plants` | Power plant details and metadata |
| `production_days` | Daily production data per power plant |
| `market_prices` | Electricity market prices by timestamp and area |
| `downtime_events` | Downtime and maintenance events |
| `work_items` | O&M work items and tasks |
| `sync_metadata` | Information about when data was last synced |

## Scheduling Regular Syncs

### Windows (Task Scheduler)

1. Open **Task Scheduler**
2. Click **Create Basic Task**
3. Name it "Portfolio Reporting Sync"
4. Set trigger (e.g., Daily at 6:00 AM)
5. Action: **Start a program**
   - Program: `python`
   - Arguments: `-m portfolio_reporting --mode incremental`
   - Start in: `C:\path\to\portfolio_reporting`

### macOS/Linux (Cron)

Add to your crontab (`crontab -e`):

```bash
# Run incremental sync every day at 6 AM
0 6 * * * cd /path/to/portfolio_reporting && python -m portfolio_reporting --mode incremental
```

## Troubleshooting

### "Module not found" Error

Make sure you installed the requirements:
```bash
pip install -r requirements.txt
```

### "Connection refused" or "Timeout" Errors

- Check that the `base_url` in `config.yaml` is correct
- Verify you have network access to the Kaia Solutions Portal
- Check that the API key is valid

### "Authentication error"

- Verify your API key in `config.yaml` is correct
- Check that your API key has not expired
- Contact your Kaia Solutions Portal administrator

### "No data returned"

- Check your `start_date` in `config.yaml` - it might be too recent
- Verify you have permission to access the power plants
- Run with `--log-level DEBUG` for more details:
  ```bash
  python -m portfolio_reporting --mode full --log-level DEBUG
  ```

### PowerBI Can't Find SQLite

PowerBI Desktop may need the SQLite connector installed:
1. Go to **File** → **Options and settings** → **Options**
2. Select **Security** → **Data Extensions**
3. Enable custom data connectors if needed

Alternatively, use the ODBC connector for SQLite.

## Advanced Options

### Custom Configuration File

```bash
python -m portfolio_reporting --config my_config.yaml
```

### Override Date Range

```bash
python -m portfolio_reporting --start-date 2024-01-01 --end-date 2024-12-31
```

### Verbose Logging

```bash
python -m portfolio_reporting --mode full --log-level DEBUG
```

### Custom Database Location

Edit `config.yaml`:
```yaml
database:
  path: "/custom/path/my_database.db"
```

## File Structure

```
portfolio_reporting/
├── config.yaml              # Your configuration (create from .example)
├── config.yaml.example      # Template configuration
├── README.md               # This file
├── requirements.txt        # Python dependencies
├── pyproject.toml         # Poetry configuration
├── verify_data.py         # Data verification script
├── data/                  # Database storage
│   └── portfolio_report.db
├── logs/                  # Log files
│   └── portfolio_reporting.log
└── src/
    └── portfolio_reporting/
        ├── __main__.py    # Main entry point
        ├── cli.py         # Command-line interface
        ├── sync.py        # Sync coordinator
        ├── api/           # API client
        ├── database/      # Database handling
        └── fetchers/      # Data fetchers
```

## Getting Help

If you encounter issues:

1. Check the log file: `logs/portfolio_reporting.log`
2. Run verification: `python verify_data.py`
3. Try with debug logging: `--log-level DEBUG`
4. Contact your system administrator or the development team

## Data Refresh Strategy

### For Daily Reports
- Run incremental sync once per day (e.g., 6 AM)
- PowerBI reports will always have yesterday's data

### For Real-Time Dashboards
- Run incremental sync every hour
- Set PowerBI to refresh more frequently

### For Historical Analysis
- Run full sync once per month to ensure data consistency
- Use incremental sync for daily updates in between

## Security Notes

- **Keep your API key secure!** Never commit `config.yaml` to version control
- The `config.yaml.example` file does not contain sensitive information
- Store the database file securely as it contains your business data

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For technical support or feature requests, contact the development team.

---

**Version:** 1.0.0
**Last Updated:** December 2025

## Multi-Currency Support

Production data is automatically fetched in both **NOK and EUR** currencies. The database stores separate records for each currency with the same production volume but different revenue calculations.

### Production Data Fields

The `production_days` table includes:
- `volume` - Production volume in MWh
- `revenue` - Revenue in the specified currency
- `currency` - Currency code (NOK or EUR)
- `forecasted_volume` - Forecasted production
- `cap_theoretical_volume` - Theoretical capacity production
- `full_load_count` - Hours at full load
- `no_load_count` - Hours at no load
- `operational_count` - Hours operational

### In PowerBI

You can filter by currency or compare revenues across currencies:

```DAX
NOK Revenue = CALCULATE(SUM(production_days[revenue]), production_days[currency] = "NOK")
EUR Revenue = CALCULATE(SUM(production_days[revenue]), production_days[currency] = "EUR")
```

