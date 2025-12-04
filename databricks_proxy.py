"""
Databricks Proxy Server
Simple Flask server to bypass CORS restrictions for Databricks API calls
"""
from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import json

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Databricks configuration
DATABRICKS_CONFIG = {
    'host': 'dbc-a3122420-cda0.cloud.databricks.com',
    'token': 'dapiebca5706c83ec913f50c0ab9e15e1e8c',
    'warehouse_id': 'b0b9bd45f760040f'
}

@app.route('/api/order', methods=['POST'])
def create_order():
    """Receive order data and send it to Databricks"""
    try:
        order_data = request.json
        print(f"Received order: {json.dumps(order_data, indent=2)}")
        
        # Create SQL statement
        sql_statement = f"""
            INSERT INTO default.default.regelkast_bestellingen
            (order_id, modelnaam, locatie, serienummer, leverancier, prijs, bestel_datum, created_at)
            VALUES (
                '{order_data['orderId']}',
                '{escape_sql(order_data['modelnaam'])}',
                '{escape_sql(order_data['locatie'])}',
                '{escape_sql(order_data['serienummer'])}',
                '{escape_sql(order_data['leverancier'])}',
                {order_data['prijs']},
                '{order_data['bestelDatum']}',
                current_timestamp()
            )
        """
        
        # Databricks SQL API endpoint
        api_url = f"https://{DATABRICKS_CONFIG['host']}/api/2.0/sql/statements"
        
        # Request body
        request_body = {
            'warehouse_id': DATABRICKS_CONFIG['warehouse_id'],
            'statement': sql_statement,
            'wait_timeout': '30s'
        }
        
        print(f"Sending to Databricks: {sql_statement}")
        
        # Make request to Databricks
        response = requests.post(
            api_url,
            headers={
                'Authorization': f"Bearer {DATABRICKS_CONFIG['token']}",
                'Content-Type': 'application/json'
            },
            json=request_body,
            timeout=35
        )
        
        if response.ok:
            result = response.json()
            print(f"Success! Response: {json.dumps(result, indent=2)}")
            return jsonify({
                'success': True,
                'message': 'Order successfully saved to Databricks',
                'orderId': order_data['orderId'],
                'databricks_response': result
            }), 200
        else:
            error_text = response.text
            print(f"Error from Databricks: {response.status_code} - {error_text}")
            return jsonify({
                'success': False,
                'error': f"Databricks error: {response.status_code}",
                'details': error_text
            }), response.status_code
            
    except Exception as e:
        print(f"Exception occurred: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def escape_sql(value):
    """Escape single quotes for SQL"""
    return value.replace("'", "''")

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'service': 'databricks-proxy'}), 200

if __name__ == '__main__':
    print("=" * 60)
    print("🚀 Databricks Proxy Server Starting...")
    print("=" * 60)
    print(f"Server will run on: http://localhost:5000")
    print(f"Databricks host: {DATABRICKS_CONFIG['host']}")
    print(f"Warehouse ID: {DATABRICKS_CONFIG['warehouse_id']}")
    print("=" * 60)
    app.run(debug=True, host='0.0.0.0', port=5000)
