# Database Autofill Project

## Setup

1. **Create Virtual Environment & Install Dependencies:**
   ```bash
   python -m venv venv
   # Windows:
   venv\Scripts\activate
   # Linux/Mac:
   source venv/bin/activate
   
   pip install -r requirements.txt
   pip install -e .
   ```

## Configuration

1. Open `examples/config.json`.
2. Update the `database` section with your PostgreSQL credentials:
   ```json
   "database": {
     "host": "localhost",
     "port": 5432,
     "name": "your_db_name",
     "user": "your_username",
     "password": "your_password"
   }
   ```
3. Update the `tables` section with the tables you want to populate and the number of rows.

## Usage

Run the tool using the `autofill` command from your virtual environment:

```bash
# Default config (examples/config.json)
autofill

# Custom config
autofill --config path/to/your/config.json
```

## Troubleshooting

- **Connection Failed**: Ensure your PostgreSQL server is running and the credentials in `config.json` are correct.
- **Command Not Found**: Make sure your virtual environment is activated (`venv\Scripts\activate`) or run via `venv\Scripts\autofill`.
